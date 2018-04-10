'use strict'

const Rx = require('rxjs');

class RetrieveNewAggregateResult{
    constructor(ids, info){
        /**
         * Aggregate ids
         */
        this.ids = [];
        this.info = {
            moreResults: true
        }
    }
}

module.exports = RetrieveNewAggregateResult;