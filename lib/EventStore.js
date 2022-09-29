'use strict';

const { of,forkJoin } = require('rxjs');
//const { } = require('rxjs/operators');
const EventResult = require('./entities/Event');

class EventStore {

    /**
     * Create a new EventStore
     * 
     * @param {Object} brokerConfig 
     *    {
     *        type,
     *        eventsTopic,
     *        brokerUrl,
     *        eventsTopicSubscription,
     *        projectId,
     *        disableListener,
     *        preParseFilter
     *    }         
     * @param {Object} storeConfig 
     *      {
     *        type,
     *        url,
     *        eventStoreDbName,
     *        aggregatesDbName
     *       }
     */
    constructor(brokerConfig, storeConfig) {
        this.brokerConfig = brokerConfig;
        this.storeConfig = storeConfig;
        if (brokerConfig) {
            switch (brokerConfig.type) {
                case "PUBSUB":
                    const PubSubBroker = require('./broker/PubSubBroker');
                    this.broker = new PubSubBroker(brokerConfig);
                    break;
                case "MQTT":
                    const MqttBroker = require('./broker/MqttBroker');
                    this.broker = new MqttBroker(brokerConfig);
                    break;
                default:
                    throw new Error(`Invalid EventStore broker type: ${brokerConfig.type} `);
            }
        }
        if (storeConfig) {
            switch (storeConfig.type) {
                case "MONGO":
                    const MongoStore = require('./store/MongoStore');
                    this.storeDB = new MongoStore(storeConfig);
                    break;
                default:
                    throw new Error(`Invalid EventStore store type: ${storeConfig.type} `);
            }
        }
    }

    /**
     * Starts Event Broker + Store
     */
    start$() {
        return forkJoin([
            !this.brokerConfig ? of({}) : this.broker.start$(),
            !this.storeConfig ? of({}) : this.storeDB.start$()
        ]);
    }

    /**
     * Stops Event Broker + Store
     */
    stop$() {
        return forkJoin([
            !this.brokerConfig ? of({}) : this.broker.stop$(),
            !this.storeConfig ? of({}) : this.storeDB.stop$()
        ]);
    }

    /**
     * Emits an Event (or an Array of events) through the message broker
     * 
     * @param {Event} event Event (or Array of Events) to emit
     * 
     * Returns an obseravable that resolves to: the same input event/events     
     */
    emitEvent$(event, { autoAcknowledgeKey } = {}) {        
        return this.broker.publish$(event);
    }

    /**
     * Stores an Event (or an Array of events) into the database
     * 
     * @param {Event} event Event (or Array of Events) to store
     * 
     * Returns an obseravable that resolves to: the same input event/events     
     */
    storeEvent$(event) {
        return this.storeDB.pushEvent$(event);
    }

    /**
     * @deprecated - not yet re-implemented
     * Find all events of an especific aggregate
     * @param {String} aggregateType Aggregate type
     * @param {String} aggregateId Aggregate Id
     * @param {number} version version to recover from (exclusive), defualt = 0
     * @param {limit} limit max number of events to return, default = 20
     * 
     * Returns an Observable that emits each found event one by one
     */
    retrieveEvents$(aggregateType, aggregateId, version = 0, limit = 20) {
        return this.storeDB.getEvents$(aggregateType, aggregateId, version, limit);
    }

    /**
     * @deprecated - not yet re-implemented
     * Find all events of an especific aggregate having taken place but not acknowledged,
     * @param {String} aggregateType Aggregate type
     * @param {string} key process key (eg. microservice name) that acknowledged the events
     * 
     * Returns an Observable that emits each found event one by one
     */
    retrieveUnacknowledgedEvents$(aggregateType, key) {
        return this.storeDB.retrieveUnacknowledgedEvents$(aggregateType, key);
    }

    /**
     * @deprecated - not yet re-implemented
     * Find Aggregates that were created after the given date
     * 
     * @param {string} type 
     * @param {number} createTimestamp 
     * @param {Object} ops {offset,pageSize}
     * 
     * Returns an observable that publish every found aggregate 
     */
    findAgregatesCreatedAfter$(type, createTimestamp = 0) {
        return this.storeDB.findAgregatesCreatedAfter$(type, createTimestamp);
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType (or Array of aggregateType)
     * @param {string} aggregateType aggregateType (or Array of aggregateType) to filter
     */
    getEventListener$(aggregateType, key, ignoreSelfEvents = true) {
        return this.broker.getEventListener$(aggregateType, ignoreSelfEvents);        
    }

    /**
     * Config aggregate event map
     * @param {*} aggregateEventsMap
     */
    configAggregateEventMap(aggregateEventsMap) {
        return this.broker.configAggregateEventMap(aggregateEventsMap);
    }

    /**
     * @deprecated - not yet re-implemented
     * @param {Event} event event to acknowledge 
     * @param {string} key process key (eg. microservice name) that is acknowledging the event
     */
    acknowledgeEvent$(event, key) {
        return of(event);
        //TODO: should accumulate and send somo sort of signal for some-one else to register the acknowledge
        //return this.storeDB.acknowledgeEvent$(event, key);
    }

    /**
     * @deprecated - not yet re-implemented
     * Ensure the existence of a registry on the ack database for an aggregate type
     * @param {string} aggregateType 
     * @param {string} key backend key 
     */
    ensureAcknowledgeRegistry$(aggregateType, key) {
        return of('ensureAcknowledgeRegistry - not yet implemented');
        //return this.storeDB.ensureAcknowledgeRegistry$(aggregateType, key);
    }
}

module.exports = EventStore;