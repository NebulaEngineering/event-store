'use strict';

const { uniqueId } = require('@nebulae/backend-node-tools').uniqueId;
const os = require("os");
const HOSTNAME = os.hostname();
const PRODUCER = process.env.MICROBACKEND_KEY || 'ms-unknown_mbe-unknown';
const [SERVICE, BACKEND] = PRODUCER.split('_');

class Event {
    constructor({ eventType, eventTypeVersion, aggregateType, aggregateId, data, user, aggregateVersion, ephemeral = false, timestamp =  Date.now() }) {        
        /**
         * Event unique ID
         */
        this.id = uniqueId.generateUInt64BE(timestamp).toString();
        /**
         * Event type
         */
        this.et = eventType;
        /**
         * Event type version
         */
        this.etv = eventTypeVersion;
        /**
         * Aggregate Type
         */
        this.at = aggregateType;
        /**
         * Aggregate ID
         */
        this.aid = aggregateId;
        /**
         * Event data
         */
        this.data = data;
        /**
         * Responsible user
         */
        this.user = user;
        /**
         * TimeStamp
         */
        this.timestamp =  timestamp;
        /**
         * Aggregate version
         */
        this.av = aggregateVersion || this.timestamp;
        /**
        * if ephemeral is true, this event will not be stored 
        */
        this.ephemeral = ephemeral;

        /**
         * Event publisher data.
         */
        this.prod ={
            /**
             * micro-service name
             */
            mse: SERVICE,
            /**
             * micro-backend name
             */
            mbe: BACKEND,
            /**
             * hostname
             */
            host: HOSTNAME
        };
    }
}

module.exports = Event;