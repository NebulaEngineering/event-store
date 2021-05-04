'use strict'

const Rx = require('rxjs');
const { filter, map, tap } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub } = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const messageAckAfterProcessed = process.env.MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.MESSAGE_ACK_AFTER_PROCESSED == "true") : null;

class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription, disableListener = false }) {
        //this.projectId = projectId;
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;
        this.disableListener = disableListener;
        this.aggregateEventsMap = null;
        this.unackedMessages = [];
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Rx.BehaviorSubject();
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
                        ConsoleLogger.d(`Unacked cronjob executed: ${self.unackedMessages ? Object.keys(self.unackedMessages).length : 'null'}`)
                        if (self.unackedMessages) {
                            Object.keys(self.unackedMessages).forEach(key => {
                                if (self.unackedMessages[key].timestamp <= (Date.now() - (2 * 60 * 1000))) {
                                    ConsoleLogger.w(`Unacked message: ${JSON.stringify(self.unackedMessages[key].data)}`)
                                }
                            });
                        }

                    }
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
            this.topic.publisher().publish(
                dataBuffer,
                { senderId: this.senderId }))
            //.do(messageId => console.log(`PubSub Message published through ${this.topic.name}, Message=${JSON.stringify(data)}`))
            ;
    }

    /**
     * Config aggregate event map
     * @param {*} aggregateEventsMap 
     */
    configAggregateEventMap(aggregateEventsMap) {
        this.aggregateEventsMap = aggregateEventsMap;
    }

    resendUnackedMessages() {
        this.unackedMessages.forEach(({ msgEvt }) => this.incomingEvents$.next(msgEvt))
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     * @param {string} allAggregateTypes all of the aggregate types are being listening 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        setTimeout(() => this.resendUnackedMessages(), 100);//resend un--listened messages
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg)
            , filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            , map(msg => ({ ...msg.data, acknowledgeMsg: msg.acknowledgeMsg }))
            , filter(evt => evt.at === aggregateType || aggregateType == "*")
        )

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
        this.messageListenerSubscription = this.getSubscription$()
            .subscribe(
                (pubSubSubscription) => {
                    this.onMessage = message => {
                        //message.ack();

                        const msgEvt = {
                            data: JSON.parse(message.data),
                            id: message.id,
                            attributes: message.attributes,
                            correlationId: message.attributes.correlationId
                        };
                        ConsoleLogger.d(`Received message, Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`)

                        const eventTypeConfig = this.aggregateEventsMap[msgEvt.data.at] ? this.aggregateEventsMap[msgEvt.data.at][msgEvt.data.et] : null;

                        if (!eventTypeConfig) {
                            // If there are not handler for this message, it means that this microservice is not interested on this information
                            //ConsoleLogger.d(`ACK Before: Message does not matter for this backend, Id: ${msgEvt.id}, at: ${msgEvt.data.at}, et: ${msgEvt.data.et}`)
                            message.ack();
                        } else {
                            const autoAck = eventTypeConfig.autoAck;
                            const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;

                            if ((autoAck != null && autoAck)
                                || processOnlyOnSync
                                || (autoAck == null && messageAckAfterProcessed != null && !messageAckAfterProcessed)) {
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
                                }
                            }

                            this.incomingEvents$.next(msgEvt);
                        }
                        //console.log(`Execute ack message ${message.id}:`);                                                                                  
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