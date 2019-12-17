'use strict'

const Rx = require('rxjs');
const { filter, map, tap} = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const PubSub = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const messageAckAfterProcessed = process.env.MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.MESSAGE_ACK_AFTER_PROCESSED == "true") : null;

class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription }) {
        //this.projectId = projectId;
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;
        this.aggregateEventsMap = null;
        this.unackedMessages = [];
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Rx.BehaviorSubject();
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        )
            // .groupBy(msg => msg.data.at)
            // .mergeMap(groupStream =>
            //     groupStream.bufferWhen(() => groupStream.debounceTime(250))
            //         .filter(bufferedArray => bufferedArray && bufferedArray.length > 0)
            //         .map(bufferedArray => bufferedArray.sort((o1, o2) => { return o1.data.av - o2.data.av }))
            //         .mergeMap(bufferedArray => Rx.Observable.from(bufferedArray))
            // );
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
            this.startMessageListener();

            //Each 5 minutes checks the unacked messages that has been enqueued and had been there for more than 5 minutes
            this.intervalID = setInterval(
                (function(self) {         //Self-executing func which takes 'this' as self
                    return function() {   //Return a function in the context of 'self'
                        
                        if(self.unackedMessages){
                            self.unackedMessages.forEach(unackedMessage => {
        
                                if(unackedMessage.timestamp <=  (Date.now()-(1*60*1000))){
                                    ConsoleLogger.w(`Unacked message: ${JSON.stringify(unackedMessage)}`)
                                }
                            });   
                        } 

                    }
                })(this),
                (1 * 60 * 1000)
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

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     * @param {string} allAggregateTypes all of the aggregate types are being listening 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true, allAggregateTypes) {
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg)
            ,filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            ,map(msg => ({...msg.data, acknowledgeMsg: msg.acknowledgeMsg}))
            // ,tap(evt => {
            //     const isListeningAggregateType = allAggregateTypes && allAggregateTypes.find(item => item === evt.at);
            //     // Aggregate types that are not being listened must be ack
            //     if(!isListeningAggregateType && evt.acknowledgeMsg){
            //         // console.log('getEventListener => ', evt.at);
            //         evt.acknowledgeMsg();                    
            //     }
            // })
            ,filter(evt => evt.at === aggregateType || aggregateType == "*")
            )
    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    getSubscription$() {
        return Rx.defer(() =>
            this.topic.subscription(this.eventsTopicSubscription)
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
                        //console.log(`Received message ${message.id}:`);
                                                
                        const msgEvt = {
                            data: JSON.parse(message.data),
                            id: message.id,
                            attributes: message.attributes,
                            correlationId: message.attributes.correlationId                            
                        };    
                        const eventTypeConfig = this.aggregateEventsMap[msgEvt.data.at] ? this.aggregateEventsMap[msgEvt.data.at][msgEvt.data.et]: null;
                        
                        console.log('aggregateEventsMap => ', this.aggregateEventsMap[msgEvt.data.at]);
                        console.log('msgEvt.data.at => ', msgEvt.data.at);
                        console.log('msgEvt.data.et => ', msgEvt.data.et);
                        console.log('eventTypeConfig => ', eventTypeConfig);
                        if(!eventTypeConfig) {
                            // If there are not handler for this message, it means that this microservice is not interested on this information
                            ConsoleLogger.d(`ACK Before: Message does not matter for this backend ${msgEvt.data.at} ${msgEvt.data.et}`)
                            message.ack();
                        }else{
                            const autoAck = eventTypeConfig.autoAck;  
                            const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;                      

                            if((autoAck != null && autoAck)
                                || processOnlyOnSync
                                || (autoAck == null && messageAckAfterProcessed != null && !messageAckAfterProcessed)) {
                                ConsoleLogger.d(`ACK Before; aggregateType: ${msgEvt.data.at}, eventType: ${msgEvt.data.et}, autoAck: ${autoAck}, processOnlyOnSync: ${processOnlyOnSync}, messageAckAfterProcessed: ${messageAckAfterProcessed}`);
                                message.ack();                                
                            }else{
                                if(!this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`]) {
                                    this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`] = {
                                        data: msgEvt.data,
                                        timestamp: Date.now()
                                    };
                                }
                                
                                msgEvt.acknowledgeMsg = () => {
                                    delete this.unackedMessages[`${msgEvt.data.at}.${msgEvt.data.et}.${msgEvt.id}`];     
                                    ConsoleLogger.d(`ACK After; aggregateType: ${msgEvt.data.at}, eventType: ${msgEvt.data.et}`);                                                          
                                    message.ack();
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