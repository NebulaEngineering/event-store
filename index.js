'use strict'

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

const EventStore = require('./lib/EventStore');
const Event = require('./lib/entities/Event');
const RetrieveEventResult = require('./lib/entities/RetrieveEventResult');
const RetrieveNewAggregateResult = require('./lib/entities/RetrieveNewAggregateResult');


module.exports = {
    EventStore,
    Event,
    RetrieveEventResult,
    RetrieveNewAggregateResult
};