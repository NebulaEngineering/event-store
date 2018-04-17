'use strict'

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

const EventStore = require('./lib/EventStore').EventStore;
const Event = require('./lib/entities/Event').Event;


module.exports = {
    EventStore,
    Event,
};