'use strict';

const { Subject, Observable, defer, from, of } = require('rxjs');
const { filter, map, mapTo, mergeMap, last } = require('rxjs/operators');
const uuidv4 = require('uuid/v4');



class MqttBroker {

    constructor({ eventsTopic, brokerUrl, disableListener = false, preParseFilter = null }) {

        this.topicName = eventsTopic;
        this.disableListener = disableListener;
        this.preParseFilter = preParseFilter;
        this.mqttServerUrl = brokerUrl;
        this.senderId = uuidv4();
        /**
         * Rx Subject for Incoming events
         */
        this.incomingEvents$ = new Subject();
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        );
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
        return Observable.create(observer => {
            this.mqttClient = this.mqtt.connect(this.mqttServerUrl);
            observer.next('MQTT broker connecting ...');
            this.mqttClient.on('connect', () => {
                observer.next('MQTT broker connected');
                this.mqttClient.subscribe(this.topicName);
                observer.next(`MQTT broker listening messages`);
                observer.complete();
            });

            if (this.disableListener) {
                return;
            }

            this.mqttClient.on('message', (topic, message) => {
                const envelope = JSON.parse(message);


                //if the message already has the headers/attributes we can skip the message before parsing
                if (envelope.attributes.at != null && envelope.attributes.at != '' && envelope.attributes.et != null && envelope.attributes.et != '') {
                    if ((this.aggregateEventsMap[envelope.attributes.at] || {})[envelope.attributes.et] == null) {
                        return;
                    }
                }

                //if the developer assigned preParse filters using the attributes (Eg. ReplciaSet instance filter) the we can discard messages before parsing
                if (envelope.attributes.aid != null && envelope.attributes.aid != '' && this.preParseFilter != null && !this.preParseFilter(envelope.attributes)) {
                    return;
                }

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
        return defer(() => this.mqttClient.end());
    }

    /**
     * Config aggregate event map
     * @param {*} aggregateEventsMap 
     */
    configAggregateEventMap(aggregateEventsMap) {
        this.aggregateEventsMap = aggregateEventsMap;
    }


    /**
     * Publish data (or an array of data) throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {*} data Object or Array of objects to send
     */
    publish$(data) {
        return (Array.isArray(data) ? from(data) : of(data)).pipe(
            mergeMap(d => defer(() => {
                this.mqttClient.publish(
                    `${this.topicName}`,
                    JSON.stringify(
                        {
                            id: d.id || '',
                            d,
                            attributes: {
                                senderId: this.senderId,
                                id: d.id || '',
                                et: d.et || '',
                                at: d.at || '',
                                aid: String(d.aid || ''),
                                etv: String(d.etv || ''),
                                ephemeral: String(d.ephemeral || false)
                            }
                        }
                    ),
                    { qos: 1 });
            }
            )),
            last(),
            mapTo(data)
        );
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg),
            filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId),
            map(msg => ({ ...msg.data, acknowledgeMsg: msg.acknowledgeMsg })),
            filter(evt => evt.at === aggregateType || aggregateType == "*")
        );
    }

}

module.exports = MqttBroker;