'use strict'

const Rx = require('rxjs');
const uuidv4 = require('uuid/v4');


class MqttBroker {

    constructor({ eventsTopic, brokerUrl, projectId }) {

        this.topicName = eventsTopic;
        this.mqttServerUrl = brokerUrl;
        this.projectId = projectId;
        this.senderId = uuidv4();
        /**
         * Rx Subject for Incoming events
         */
        this.incomingEvents$ = new Rx.BehaviorSubject();

        /**
         * MQTT Client
         */
        const mqtt = require('mqtt');
        this.mqttClient = mqtt.connect(this.mqttServerUrl);
        this.configMessageListener();
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

        return Rx.Observable.of(0)
            .map(() => {
                this.mqttClient.publish(`${this.projectId}/${this.topicName}`, dataBuffer, { qos: 0 });
                return uuid;
            })
            //.do(messageId => console.log(`Message published through ${this.topicName}, MessageId=${messageId}`))
            ;
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        return this.incomingEvents$
            .filter(msg => msg)
            .filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            .map(msg => msg.data)
            .filter(evt => evt.at === aggregateType)
    }


    /**
     * Configure to listen messages
     */
    configMessageListener() {

        const that = this;

        this.mqttClient.on('connect', function () {
            that.mqttClient.subscribe(`${that.projectId}/${that.topicName}`);
            //console.log(`Mqtt client subscribed to ${that.projectId}/${that.topicName}`);
        });

        this.mqttClient.on('message', function (topic, message) {
            const envelope = JSON.parse(message);
            //console.log(`Received message id: ${envelope.id}`);
            // message is Buffer
            that.incomingEvents$.next(
                {
                    id: envelope.id,
                    data: envelope.data,
                    attributes: envelope.attributes,
                    correlationId: envelope.attributes.correlationId
                }
            );
        })
    }

    stopListening(){
        this.mqttClient.end();
    }
    
}

module.exports = MqttBroker;