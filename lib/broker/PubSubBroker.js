'use strict';

const Rx = require('rxjs');
const { filter, map, tap, delay, mergeMap } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub } = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const messageAckAfterProcessed = process.env.MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.MESSAGE_ACK_AFTER_PROCESSED == "true") : null;

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
        this.incomingEvents$ = new Rx.BehaviorSubject();
        this.aggregateEventsMapConfiguredSubject$ = new Rx.BehaviorSubject();
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        );
        this.senderId = uuidv4();

        this.pubsubClient = new PubSub({
            //projectId: projectId,
        });

        this.topic = this.pubsubClient.topic(eventsTopic);
    }

    /**
     * Starts Broker connections
     * Returns an Obserable that resolves to each connection result
     */
    start$() {
        return Rx.Observable.create(observer => {
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
        return Rx.Observable.create(observer => {

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
     * Publish data throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {Object} data 
     */
    publish$(data) {
        const dataBuffer = Buffer.from(JSON.stringify(data));
        return Rx.defer(() =>
            this.topic.publisher.publish(
                dataBuffer,
                {
                    senderId: this.senderId || '',
                    et: data.et || '',
                    at: data.at || '',
                    aid: data.aid || '',
                    etv: data.etv || '',
                    ephemeral: data.ephemeral || false
                }))
            //.do(messageId => console.log(`PubSub Message published through ${this.topic.name}, Message=${JSON.stringify(data)}`))
            ;
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
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     * @param {string} allAggregateTypes all of the aggregate types are being listening 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
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
            filter(evt => evt.at === aggregateType || aggregateType == "*")
        );

    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    getSubscription$() {
        const opts = {
            flowControl: {
                maxMessages: 10000
            }
        };
        return Rx.defer(() =>
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
                        //if the message already has the headers/attributes we can skip the message before parsing
                        if (message.attributes.at != null && message.attributes.at!='' && message.attributes.et != null && message.attributes.et != '') {
                            if ((this.aggregateEventsMap[message.attributes.at] || {})[message.attributes.et] == null) {
                                message.ack();
                                return;
                            }
                        }

                        //if the developer assigned preParse filters using the attributes (Eg. ReplciaSet instance filter) the we can discard messages before parsing
                        if(this.preParseFilter != null && !this.preParseFilter(message.attributes)){
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

                        const eventTypeConfig = this.aggregateEventsMap[msgEvt.data.at] ? this.aggregateEventsMap[msgEvt.data.at][msgEvt.data.et] : null;

                        if (!eventTypeConfig) {
                            // If there are not handler for this message, it means that this microservice is not interested on this information
                            //ConsoleLogger.d(`ACK Before: Message does not matter for this backend, Id: ${msgEvt.id}, at: ${msgEvt.data.at}, et: ${msgEvt.data.et}`)
                            message.ack();
                            return;
                        }


                        const autoAck = eventTypeConfig.autoAck;
                        const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;

                        if ((autoAck != null && autoAck) || processOnlyOnSync || (autoAck == null && messageAckAfterProcessed != null && !messageAckAfterProcessed)) {
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