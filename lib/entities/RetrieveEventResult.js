'use strict'

const Rx = require('rxjs');

class RetrieveEventResult{
    constructor(){
        /**
         * Events of type Event
         */
        this.events = [];
        this.info = {
            moreResults: true
        }
    }
}

module.exports = RetrieveEventResult;