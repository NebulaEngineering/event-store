'use strict'

const Rx = require('rxjs');

class MongoStore {

    constructor({ }) {
    }

    /**
     * Find Aggragates events and returns a promise of RetrieveEventResult
     * @param {string} aggregateType 
     * @param {string} aggregateId 
     * @param {number} aggregateVersion 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findEvents(aggregateType, aggregateId, aggregateVersion, {offset,pageSize}){

    }

    /**
     * Find Aggragates that were created after the given date and returns a promise of RetrieveNewAggregateResult
     * @param {string} aggregateType 
     * @param {number} createTimestamp 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findAgregatesCreatedAfter(aggregateType, createTimestamp,{offset,pageSize}){
        
    }
}

module.exports = MongoStore;