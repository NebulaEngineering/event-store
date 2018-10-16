'use strict'

const Rx = require('rxjs');
const { filter, mapTo, map} = require('rxjs/operators');
const uuidv4 = require('uuid/v4');



class MqttBroker {

    constructor({ eventsTopic, brokerUrl }) {

        this.topicName = eventsTopic;
        this.mqttServerUrl = brokerUrl;
        this.senderId = uuidv4();
        /**
         * Rx Subject for Incoming events
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
            // )
            ;

        /**
         * MQTT Client
         */
        this.mqtt = require("async-mqtt");
    }

    /**
     * Starts Broker connections
     * Returns an Obserable that resolves to each connection result
     */
    start$() {
        return Rx.Observable.create(observer => {
            this.mqttClient = this.mqtt.connect(this.mqttServerUrl);
            observer.next('MQTT broker connecting ...');
            this.mqttClient.on('connect', () => {
                observer.next('MQTT broker connected');
                this.mqttClient.subscribe(this.topicName);
                observer.next(`MQTT broker listening messages`);
                observer.complete();
            });
            this.mqttClient.on('message', (topic, message) => {
                const envelope = JSON.parse(message);
                //console.log(`**************** Received message id: ${envelope.id}`);
                // message is Buffer
                this.incomingEvents$.next(
                    {
                        id: envelope.id,
                        data: envelope.data,
                        attributes: envelope.attributes,
                        correlationId: envelope.attributes.correlationId
                    }
                );
            });


        });


    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected
     */
    stop$() {
        return Rx.defer(() => this.mqttClient.end());
    }


    /**
     * Publish data throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {Object} data 
     */
    publish$(data) {
        const uuid = uuidv4();
        const dataBuffer = JSON.stringify(
            {
                id: uuid,
                data,
                attributes: {
                    senderId: this.senderId
                }
            }
        );
        return Rx.defer(() => this.mqttClient.publish(`${this.topicName}`, dataBuffer, { qos: 1 }))
            .pipe(mapTo(uuid));
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg)
            ,filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            ,map(msg => msg.data)
            ,filter(evt => evt.at === aggregateType || aggregateType == "*" )
        )
    }

}

module.exports = MqttBroker;