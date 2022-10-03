'use strict';

const { Subject, BehaviorSubject, Observable, defer, from, of } = require('rxjs');
const { filter, map, tap, mergeMap, last, mapTo } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub } = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED = process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED == "true") : null;
const EVENT_STORE_BROKER_MAX_MESSAGES = parseInt(process.env.EVENT_STORE_BROKER_MAX_MESSAGES || "1000");
const EVENT_STORE_BROKER_MAX_MESSAGES_PERCENTAGE_WARNING_THRESHOLD = parseInt(process.env.EVENT_STORE_BROKER_MAX_MESSAGES_PERCENTAGE_WARNING_THRESHOLD || "50");
const EVENT_STORE_BROKER_MAX_MESSAGES_PERCENTAGE_FATAL_THRESHOLD = parseInt(process.env.EVENT_STORE_BROKER_MAX_MESSAGES_PERCENTAGE_FATAL_THRESHOLD || "90");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG || "10");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS || "1000");
const EVENT_STORE_BROKER_MAX_EXTENSION = parseInt(process.env.EVENT_STORE_BROKER_MAX_EXTENSION || "1500");

class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription, disableListener = false, preParseFilter = null }) {
        //this.projectId = projectId;
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;
        this.disableListener = disableListener;
        this.preParseFilter = preParseFilter;
        this.aggregateEventsMap = null;
        this.unackedMessages = [];
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Subject();
        this.aggregateEventsMapConfiguredSubject$ = new BehaviorSubject();
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        );
        this.senderId = uuidv4();
        this.pubsubClient = new PubSub({});
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
                        ConsoleLogger.d(`Unacked cronjob executed: ${self.unackedMessages ? Object.keys(self.unackedMessages).length : 'null'}`);
                        if (self.unackedMessages) {
                            Object.keys(self.unackedMessages).forEach(key => {
                                if (self.unackedMessages[key].timestamp <= (Date.now() - (2 * 60 * 1000))) {
                                    ConsoleLogger.w(`EventStore.PubSubBroker.start$: Unacked message: ${JSON.stringify(self.unackedMessages[key].data)}`);
                                }
                            });
                        }

                    };
                })(this),
                (2 * 60 * 1000)
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

            if (this.disableListener) {
                observer.next(`Event Store onStop PubSub Broker listener DISABLED`);
                observer.complete();
                return;
            }

            this.getSubscription$().subscribe(
                (subscription) => {
                    subscription.removeListener(`message`, this.onMessage);
                    observer.next(`Event Store PubSub Broker removed listener: Topic=${this.eventsTopic}, subscriptionName=${subscription}`);
                },
                (error) => observer.error(error),
                () => {
                    this.messageListenerSubscription.unsubscribe();
                    observer.complete();
                }
            );

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
            map(msg => ({ ...msg.data, acknowledgeMsg: msg.acknowledgeMsg })),
            filter(evt => allowAll || (isAggregateTypeAnArray ? aggregateType.includes(evt.at) : evt.at === aggregateType))
        );

    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    getSubscription$() {
        const opts = {
            flowControl: {
                maxMessages: EVENT_STORE_BROKER_MAX_MESSAGES,
                allowExcessMessages: false,
                maxExtension: EVENT_STORE_BROKER_MAX_EXTENSION,
            }
        };
        return defer(() =>
            this.topic.subscription(this.eventsTopicSubscription, opts)
                .get({ autoCreate: true })).pipe(
                    map(results => results[0])
                );
    }

    /**
     * Starts to listen messages
     */
    startMessageListener() {
        this.messageListenerSubscription =
            this.aggregateEventsMapConfiguredSubject$.pipe(
                tap(() => ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: Waiting for aggregateEventsMap to be configured before listening to messages`)),
                filter(value => value),// waits until the aggregateEventsMap is configured
                tap(() => ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: aggregateEventsMap configured, will start listening messages`)),
                mergeMap(() => this.getSubscription$())
            ).subscribe(
                (pubSubSubscription) => {
                    this.onMessage = message => {

                        const hasEventSourcingAttributes = message.attributes.at != null && message.attributes.at != '' && message.attributes.et != null && message.attributes.et != ' ' && message.attributes.aid != null && message.attributes.aid != '';
                        let eventTypeConfig;
                        //if the message already has the headers/attributes we can skip the message before parsing
                        if (hasEventSourcingAttributes) {
                            const atMap = this.aggregateEventsMap[message.attributes.at] || this.aggregateEventsMap.$ALL;
                            eventTypeConfig = !atMap ? undefined : (atMap[message.attributes.et] || atMap.$ALL);
                            if (!eventTypeConfig) {
                                message.ack();
                                return;
                            }
                        }

                        //if the developer assigned preParse filters using the attributes (Eg. ReplciaSet instance filter) the we can discard messages before parsing
                        if (hasEventSourcingAttributes && this.preParseFilter != null && !this.preParseFilter(message.attributes)) {
                            message.ack();
                            return;
                        }

                        const msgEvt = {
                            data: JSON.parse(message.data),
                            id: message.id,
                            attributes: message.attributes,
                            correlationId: message.attributes.correlationId
                        };
                        //ConsoleLogger.d(`EventStore.PubSubBroker.startMessageListener: Received message, Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`)

                        if (!eventTypeConfig) {
                            const atMap = this.aggregateEventsMap[msgEvt.data.at] || this.aggregateEventsMap.$ALL;
                            eventTypeConfig =  !atMap ? undefined : atMap[msgEvt.data.et] || atMap.$ALL;
                        }
                        if (!eventTypeConfig) {
                            // If there are not handler for this message, it means that this microservice is not interested on this information
                            //ConsoleLogger.d(`ACK Before: Message does not matter for this backend, Id: ${msgEvt.id}, at: ${msgEvt.data.at}, et: ${msgEvt.data.et}`)
                            message.ack();
                            return;
                        }


                        const autoAck = eventTypeConfig.autoAck;
                        const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;

                        if ((autoAck != null && autoAck) || processOnlyOnSync || (autoAck == null && EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null && !EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED)) {
                            // ConsoleLogger.d(`ACK Before; , Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}, autoAck: ${autoAck}, processOnlyOnSync: ${processOnlyOnSync}, messageAckAfterProcessed: ${messageAckAfterProcessed}`);
                            message.ack();
                        } else {
                            if (!this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`]) {
                                this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`] = {
                                    msgEvt,
                                    data: msgEvt.data,
                                    timestamp: Date.now()
                                };
                            }

                            const self = this;
                            msgEvt.acknowledgeMsg = () => {
                                //ConsoleLogger.d(`ACK After; , Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`);                                                          
                                message.ack();
                                delete self.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`];
                            };
                        }

                        this.incomingEvents$.next(msgEvt);
                    };
                    pubSubSubscription.on(`message`, this.onMessage);
                },
                (err) => {
                    console.error('Failed to obtain EventStore subscription', err);
                },
                () => {
                    console.log('GatewayEvents listener has completed!');
                }
            );
    }

}

module.exports = PubSubBroker;