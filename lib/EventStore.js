'use strict'

const Rx = require('rxjs');
const { filter, map, concatMap, switchMap } = require('rxjs/operators');
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

        switch (storeConfig.type) {
            case "MONGO":
                const MongoStore = require('./store/MongoStore');
                this.storeDB = new MongoStore(storeConfig);
                break;
            default:
                throw new Error(`Invalid EventStore store type: ${storeConfig.type} `);
        }
    }

    /**
     * Starts Event Broker + Store
     */
    start$() {
        return Rx.merge(
            this.broker.start$(),
            this.storeDB.start$()
        );
    }

    /**
     * Stops Event Broker + Store
     */
    stop$() {
        return Rx.merge(
            this.broker.stop$(),
            this.storeDB.stop$()
        );
    }

    /**
     * Appends and emit a new Event
     * 
     * @param {Event} event Event to store and emit
     * 
     * Returns an obseravable that resolves to:
     * {
     *  storeResult :  {aggregate,event,versionTimeStr},
     *  brokerResult: { messageId }
     * }
     * 
     * where:
     *  - aggregate = current aggregate state
     *  - event = persisted event
     *  - versionTimeStr = EventStore date index where the event was store
     *  - messageId: sent message ID
     *      
     */
    emitEvent$(event) {
        return this.storeDB.pushEvent$(event)
            .pipe(
                concatMap((storeResult) =>
                    this.broker.publish$(storeResult.event)
                        .pipe(map(messageId => {
                            return { storeResult, brokerResult: { messageId } };
                        }))
                )
            );
    }

    /**
     * Find all events of an especific aggregate
     * @param {String} aggregateType Aggregate type
     * @param {String} aggregateId Aggregate Id
     * @param {number} version version to recover from (exclusive), defualt = 0
     * @param {limit} limit max number of events to return, default = 20
     * 
     * Returns an Observable that emits each found event one by one
     */
    retrieveEvents$(aggregateType, aggregateId, version = 0, limit = 20) {
        return this.storeDB.getEvents$(aggregateType, aggregateId, version, limit)
    }

    /**
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
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     */
    getEventListener$(aggregateType, key, ignoreSelfEvents = true) {
        return this.storeDB.findLatestAcknowledgedTimestamp$(aggregateType, key).pipe(
            switchMap(latestTimeStamp => {
                //this will ignore all queued messages that were already processed
                return this.broker.getEventListener$(aggregateType, ignoreSelfEvents)
                    .pipe(filter(evt => evt.timestamp > latestTimeStamp))
            })
        );
    }

    /**
     * @param {Event} event event to acknowledge 
     * @param {string} key process key (eg. microservice name) that is acknowledging the event
     */
    acknowledgeEvent$(event, key) {
        return this.storeDB.acknowledgeEvent$(event, key);
    }

    /**
     * Ensure the existence of a registry on the ack database for an aggregate type
     * @param {string} aggregateType 
     * @param {string} key backend key 
     */
    ensureAcknowledgeRegistry$(aggregateType, key) {
        return this.storeDB.ensureAcknowledgeRegistry$(aggregateType, key);
    }
}

module.exports = EventStore;