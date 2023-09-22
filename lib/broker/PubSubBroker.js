'use strict';

const { Subject, BehaviorSubject, Observable, defer, from, of, timer, interval } = require('rxjs');
const { filter, map, tap, mergeMap, last, mapTo, bufferTime, retryWhen, delayWhen, concatMap, takeUntil, catchError, first } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub, v1 } = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const googleCredentials = require(process.env.GOOGLE_APPLICATION_CREDENTIALS || "");
const EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED = process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED == "true") : null;
const EVENT_STORE_BROKER_MAX_MESSAGES = parseInt(process.env.EVENT_STORE_BROKER_MAX_MESSAGES || "1500");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG || "1000");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS || "200");
const EVENT_STORE_BROKER_MAX_EXTENSION = parseInt(process.env.EVENT_STORE_BROKER_MAX_EXTENSION || "1500");
const EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS = parseInt(process.env.EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS || "3", 10);

const EVENT_STORE_BROKER_TIME_BUFFER = parseInt(process.env.EVENT_STORE_BROKER_TIME_BUFFER || "500", 10);
const EVENT_STORE_BROKER_AMOUNT_BUFFER = parseInt(process.env.EVENT_STORE_BROKER_AMOUNT_BUFFER || "1000", 10);

const EVENT_STORE_PULL_MESSAGE_INTERVAL_MILLIS = parseInt(process.env.EVENT_STORE_PULL_MESSAGE_INTERVAL_MILLIS || "300", 10);
const EVENT_STORE_PULL_MESSAGE_AMOUNT = parseInt(process.env.EVENT_STORE_PULL_MESSAGE_AMOUNT || "500", 10);
const EVENT_STORE_SUBSCRIPTION_ACK_DEADLINE = parseInt(process.env.EVENT_STORE_SUBSCRIPTION_ACK_DEADLINE || "500", 10);
const EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_PERIOD = parseInt(process.env.EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_PERIOD || "180000", 10);
const EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_THRESHOLD = parseInt(process.env.EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_THRESHOLD || "120000", 10);


class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription, disableListener = false, preParseFilter = null, projectId }) {

        this.projectId = projectId || googleCredentials?.project_id;
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;
        this.disableListener = disableListener;
        this.preParseFilter = preParseFilter;
        this.aggregateEventsMap = null;
        this.unackedMessages = [];
        this.currentMessages = {};
        this.isStopped = false;

        /**
         * flag to indicate if we are currently pulling messages and shoul wait
         */
        this.pullingAndWaitingForMessages = false;
        /**
         * msgWithoutAckAmount
         */
        this.msgWithoutAckAmount = 0;
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Subject();
        /**
         * Rx Subject for acknowledge messages
         */
        this.messagesToAcknowledge$ = new Subject();
        /**
         * Rx Subject for stopping listening messages
         */
        this.messageListenerDestroyer$ = new Subject();
        /**
         * Rx BehaviorSubject for starting listening messages
         */

        this.aggregateEventsMapConfiguredSubject$ = new BehaviorSubject();

        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(filter(msg => msg));

        this.senderId = uuidv4();


        this.shouldShowStopPullingMessage = false;

        this.pubsubClient = new PubSub({});
        this.subscriptionClient = new v1.SubscriberClient({});
        if(!this.disableListener){
            this.formattedSubscription = this.subscriptionClient.subscriptionPath(this.projectId, eventsTopicSubscription);
            this.subscriptionTopic = `projects/${this.projectId}/topics/${this.eventsTopic}`;
        }
        this.topic = this.pubsubClient.topic(eventsTopic, {
            batching: {
                maxMessages: EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG,
                maxMilliseconds: EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS,
            }
        }
        );
    }

    /**
     * Starts Broker connections
     * Returns an Obserable that resolves to each connection result
     */
    start$() {
        return new Observable(observer => {
            if (this.disableListener) {
                observer.next(`Event Store onStart PubSub Broker listener DISABLED`);
                observer.complete();
                return;
            }

            this.startMessageListener();

            //Each 5 minutes checks the unacked messages that has been enqueued and had been there for more than 5 minutes
            this.intervalID = setInterval(
                (function (self) {
                    return function () {
                        if (self.unackedMessages) {
                            Object.keys(self.unackedMessages).forEach(key => {
                                const {id, aid, at, et, timestamp} = self.unackedMessages[key];
                                const age = parseInt((Date.now()-timestamp)/60000);
                                if (self.unackedMessages[key].timestamp <= (Date.now() - (EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_THRESHOLD))) {
                                    ConsoleLogger.w(`EventStore.PubSubBroker.start$: Unacked message: ${JSON.stringify({id, at, et, timestamp, aid, age: `${age}m`})}`);
                                }
                            });
                        }

                    };
                })(this),
                (EVENT_STORE_OLD_UNACKED_EVENTS_MONITOR_PERIOD)
            );

            observer.next(`Event Store PubSub Broker listening: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            observer.complete();
        });
    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected
     */
    stop$() {
        return new Observable(observer => {

            if (this.messageListenerSubscription) this.messageListenerSubscription.unsubscribe();
            if (this.messagesToAcknowledgeSubscription) this.messagesToAcknowledgeSubscription.unsubscribe();

            observer.next(`Event Store onStop PubSub Broker listener DISABLED`);
            observer.complete();
        });

    }

    /**
     * Publish data (or an array of data) throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {*} event Object or Array of objects to send
     */
    publish$(event) {
        return (Array.isArray(event) ? from(event) : of(event)).pipe(
            mergeMap(d => defer(() => this.topic.publish(
                Buffer.from(JSON.stringify(d)),
                {
                    senderId: this.senderId || '',
                    id: d.id || '',
                    et: d.et || '',
                    at: d.at || '',
                    aid: String(d.aid || ''),
                    etv: String(d.etv || ''),
                    ephemeral: String(d.ephemeral || false)
                })
            )),
            last(),
            mapTo(event)
        );
    }

    /**
     * Config aggregate event map
     * @param {*} aggregateEventsMap 
     */
    configAggregateEventMap(aggregateEventsMap) {
        this.aggregateEventsMap = aggregateEventsMap;
        this.aggregateEventsMapConfiguredSubject$.next(aggregateEventsMap);
    }

    resendUnackedMessages() {
        this.unackedMessages.forEach(({ msgEvt }) => this.incomingEvents$.next(msgEvt));
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType (or Array of aggregate types)
     * @param {string} aggregateType aggregateType (or Array of aggregateType) to filter
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        const isAggregateTypeAnArray = Array.isArray(aggregateType);
        const allowAll = isAggregateTypeAnArray ? aggregateType.includes('$ALL') : aggregateType === '$ALL';
        setTimeout(() => this.resendUnackedMessages(), 100);//resend un--listened messages
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg),
            tap(msg => {
                if (ignoreSelfEvents && msg.attributes.senderId === this.senderId) {
                    msg.acknowledgeMsg();
                }
            }),
            filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId),
            map(msg => ({
                ...msg.data, acknowledgeMsg: msg.acknowledgeMsg
            })),
            filter(evt => allowAll || (isAggregateTypeAnArray ? aggregateType.includes(evt.at) : evt.at === aggregateType))
        );

    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    getSubscription$() {
        return defer(() => this.subscriptionClient.getSubscription({ subscription: [this.formattedSubscription] })).pipe(
            catchError((err) => {
                const request = {
                    name: this.formattedSubscription,
                    topic: this.subscriptionTopic,
                    ackDeadlineSeconds: EVENT_STORE_SUBSCRIPTION_ACK_DEADLINE
                };
                return this.subscriptionClient.createSubscription(request);
            })
        );
    }

    /**
     * Starts to listen messages
     */
    startMessageListener() {
        this.messageListenerSubscription =
            this.aggregateEventsMapConfiguredSubject$.pipe(
                tap(() => ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: Waiting for aggregateEventsMap to be configured before listening to messages`)),

                first(u => u),

                mergeMap(() => this.getSubscription$()),
                tap(() => {
                    this.startMessageAcknowledged();
                    ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: aggregateEventsMap configured, will start listening messages`);
                }),


                mergeMap(() => interval(EVENT_STORE_PULL_MESSAGE_INTERVAL_MILLIS)),
                takeUntil(this.messageListenerDestroyer$),
                filter(() => {
                    if (this.msgWithoutAckAmount > EVENT_STORE_BROKER_MAX_MESSAGES && !this.shouldShowStopPullingMessage) {
                        this.shouldShowStopPullingMessage = true;
                        ConsoleLogger.e(`Stop pulling messages because of messages without ack limit exceeded: ${this.msgWithoutAckAmount}, of ${EVENT_STORE_BROKER_MAX_MESSAGES}`);
                    }
                    if (this.msgWithoutAckAmount < EVENT_STORE_BROKER_MAX_MESSAGES && this.shouldShowStopPullingMessage) {
                        this.shouldShowStopPullingMessage = false;
                        ConsoleLogger.i(`Start pulling messages because of messages without ack limit flatted: ${this.msgWithoutAckAmount}, of ${EVENT_STORE_BROKER_MAX_MESSAGES}`);
                    }
                    if(this.pullingAndWaitingForMessages){
                        return false;
                    }
                    this.pullingAndWaitingForMessages = true;
                    return this.msgWithoutAckAmount < EVENT_STORE_BROKER_MAX_MESSAGES;
                }),

                mergeMap(() => {
                    const request = {
                        subscription: this.formattedSubscription,
                        maxMessages: EVENT_STORE_PULL_MESSAGE_AMOUNT,
                        allowExcessMessages: false
                    };
                    return this.subscriptionClient.pull(request);
                }),
                mergeMap(([response]) => {
                    this.pullingAndWaitingForMessages = false;
                    const messages = response.receivedMessages;
                    return from(messages);
                }),
            ).subscribe(
                (rawData) => {
                    const message = { ...rawData.message, ackId: rawData.ackId };
                    if (!this.currentMessages[rawData.message.messageId]) {
                        this.currentMessages[rawData.message.messageId] = Date.now();
                        this.msgWithoutAckAmount++;
                        const hasEventSourcingAttributes = message.attributes.at != null && message.attributes.at != '' && message.attributes.et != null && message.attributes.et != ' ' && message.attributes.aid != null && message.attributes.aid != '';
                        let eventTypeConfig;
                        //if the message already has the headers/attributes we can skip the message before parsing
                        if (hasEventSourcingAttributes) {
                            const atMap = this.aggregateEventsMap[message.attributes.at] || this.aggregateEventsMap.$ALL;
                            eventTypeConfig = !atMap ? undefined : (atMap[message.attributes.et] || atMap.$ALL);
                            if (!eventTypeConfig) {
                                // message.ack();
                                this.messagesToAcknowledge$.next(message);
                                return;
                            }
                        }
                        //if the developer assigned preParse filters using the attributes (Eg. ReplciaSet instance filter) the we can discard messages before parsing
                        if (hasEventSourcingAttributes && this.preParseFilter != null && !this.preParseFilter(message.attributes)) {
                            // message.ack();
                            this.messagesToAcknowledge$.next(message);
                            return;
                        }

                        const msgEvt = {
                            data: JSON.parse(message.data),
                            id: message.id,
                            attributes: message.attributes,
                            correlationId: message.attributes.correlationId,
                            ackId: message.ackId,
                            messageId: message.messageId
                        };

                        //ConsoleLogger.d(`EventStore.PubSubBroker.startMessageListener: Received message, Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`)

                        if (!eventTypeConfig) {
                            const atMap = this.aggregateEventsMap[msgEvt.data.at] || this.aggregateEventsMap.$ALL;
                            eventTypeConfig = !atMap ? undefined : atMap[msgEvt.data.et] || atMap.$ALL;
                        }
                        if (!eventTypeConfig) {
                            // If there are not handler for this message, it means that this microservice is not interested on this information
                            //ConsoleLogger.d(`ACK Before: Message does not matter for this backend, Id: ${msgEvt.id}, at: ${msgEvt.data.at}, et: ${msgEvt.data.et}`)
                            // message.ack();
                            this.messagesToAcknowledge$.next(message);
                            return;
                        }


                        const autoAck = eventTypeConfig.autoAck;
                        const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;

                        if ((autoAck != null && autoAck) || processOnlyOnSync || (autoAck == null && EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null && !EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED)) {
                            // ConsoleLogger.d(`ACK Before; , Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}, autoAck: ${autoAck}, processOnlyOnSync: ${processOnlyOnSync}, messageAckAfterProcessed: ${messageAckAfterProcessed}`);
                            // message.ack();
                            this.messagesToAcknowledge$.next(message);
                        } else {
                            if (!this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`]) {
                                this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`] = {
                                    msgEvt,
                                    data: msgEvt.data,
                                    timestamp: Date.now(),
                                    ackId: msgEvt.ackId,
                                    messageId: msgEvt.messageId
                                };
                            }

                            const self = this;
                            msgEvt.acknowledgeMsg = () => {
                                //ConsoleLogger.d(`ACK After; , Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`);
                                self.messagesToAcknowledge$.next({ ...msgEvt, shouldRemoveFromUnackedMessages: true });
                                // message.ack();

                            };
                        }

                        this.incomingEvents$.next(msgEvt);
                    }

                },
                (err) => {
                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageListener: Observable terminate due to error`, err);
                    process.exit(1);
                },
                () => {
                    this.isStopped = true;
                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageListener: Observable completed`);
                }
            );
    }

    /**
     * Starts to buffer and acknowledge messages
     */
    startMessageAcknowledged() {
        this.messagesToAcknowledgeSubscription = this.messagesToAcknowledge$.pipe(
            filter(msg => msg?.ackId),
            bufferTime(EVENT_STORE_BROKER_TIME_BUFFER, null, EVENT_STORE_BROKER_AMOUNT_BUFFER),
            mergeMap((bufferedMessages) => {
                let retryAttempt = 0;
                const messagesToAckIds = bufferedMessages.map(msg => msg.ackId);
                if (messagesToAckIds.length === 0) {
                    return of("NOT EVENTS");
                }
                return of(messagesToAckIds).pipe(
                    mergeMap((ackIds) => {
                        const ackRequest = {
                            subscription: this.formattedSubscription,
                            ackIds: ackIds,
                        };
                        return this.subscriptionClient.acknowledge(ackRequest);
                    }),
                    tap((result) => {
                        this.msgWithoutAckAmount -= messagesToAckIds.length;
                        for (let msg of bufferedMessages) {
                            delete this.currentMessages[msg.messageId];
                            if (msg.shouldRemoveFromUnackedMessages) delete this.unackedMessages[`${msg.data.at}.${msg.data.et}.${msg.id}`];
                        }

                    }),

                    retryWhen((errors) =>
                        errors.pipe(
                            tap((err) => {
                                retryAttempt += 1;
                                if (retryAttempt === EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS) {
                                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageAcknowledged: STOPPED`);
                                    this.messageListenerDestroyer$.next("stop");
                                }
                            }),
                            tap(() => ConsoleLogger.e(`EventStore.PubSubBroker.startMessageAcknowledged: COULD NOT ACKNOWLEDGE MESSAGES ${JSON.stringify(messagesToAckIds)}`)),
                            delayWhen(() => timer(5 * 1000))
                        )
                    ),

                );
            })
        ).subscribe(
            () => {
                if (this.isStopped) {
                    process.exit(1);
                }
            },
            (err) => {
                ConsoleLogger.e('Failed to acknowledged messages', err);
                process.exit(1);
            },
            () => {
                ConsoleLogger.e('messagesToAcknowledge has completed!');
            }
        );
    }

}

module.exports = PubSubBroker;