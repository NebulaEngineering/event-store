'use strict'

const Rx = require('rxjs');

class Event {
    constructor(aggregateType, aggregateId, eventName, data, user, metadata) {
        /**
         * Aggregate Type
         */
        this.at = aggregateType;
        /**
         * Aggregate ID
         */
        this.aid = aggregateId;
        /**
         * Event name
         */
        this.name = eventName;
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

        if (metadata) {
            /**
             * Extra info
             */
            this.metadata = metadata;
        }

    }
}

module.exports = Event;