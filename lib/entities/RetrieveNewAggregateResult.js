'use strict'

const Rx = require('rxjs');

class RetrieveNewAggregateResult{
    constructor(){
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