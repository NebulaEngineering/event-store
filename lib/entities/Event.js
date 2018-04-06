'use strict'

const Rx = require('rxjs');

class Event {
    constructor(eventType, eventTypeVersion, aggregateType, aggregateId, aggregateVersion, data, user) {
        
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
         * Aggregate version
         */
        this.av = aggregateVersion;      
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
        this.timestamp = (new Date).getTime();

        // if (metadata) {
        //     /**
        //      * Extra info
        //      */
        //     this.metadata = metadata;
        // }

    }
}

module.exports = Event;