'use strict'

const Rx = require('rxjs');
const EventResult = require('./entities/Event');
const RetrieveEventResult = require('./entities/RetrieveEventResult');
const RetrieveNewAggregateResult = require('./entities/RetrieveNewAggregateResult');

class EventStore {


    /**
     * Create a new EventStore
     * 
     * @param {Object} brokerConfig 
     *    {
             type,
             eventsTopic,
             brokerUrl,
             eventsTopicSubscription,
             projectId,
         }         
     * @param {Object} storeConfig 
     *      store : {
             type,
             connString
            }
     */
    constructor(brokerConfig, storeConfig) {        
        switch (brokerConfig.type) {
            case "PUBSUB":
                const PubSubBroker = require('./broker/PubSubBroker');
                this.broker = new PubSubBroker(brokerConfig);
                break;
            case "MQTT":
                const MqttBroker = require('./broker/MqttBroker');
                this.broker = new MqttBroker(brokerConfig);  
                break;
        }
    }

    /**
     * Appends and emit a new Event
     * @param {Event} event 
     */
    emitEvent(event) {
        return this.broker.publish$(event).toPromise();
    }

    /**
     * 
     * Query and retrieves all aggregate events after the given version
     * @param {string} aggregateType 
     * @param {string} aggregateId 
     * @param {number} aggregateVersion 
     * 
     * returns promise <RetrieveEventResult>
     */
    retrieveEvents(aggregateType, aggregateId, aggregateVersion) {
        return Rx.Observable.of(new RetrieveEventResult()).toPromise();
    }

    /**
     * Query and retrieves all aggregate ids created after the given date
     * @param {string} aggregateType 
     * @param {number} timestamp epoch millis 
     * 
     * returns promise <RetrieveNewAggregateResult>
     */
    retrieveNewAggregates(aggregateType, timestamp) {
        return Rx.Observable.of(new RetrieveNewAggregateResult()).toPromise();
    }

}

module.exports = EventStore;