// TEST LIBS
const assert = require('assert');
const Rx = require('rxjs');

//LIBS FOR TESTING
const EventStore = require('../lib/EventStore');
const Event = require('../lib/entities/Event');

//GLOABAL VARS to use between tests
let eventStore = {};
let event = new Event('Test', 1, 'TestCreated', { id: 1, name: 'x' }, 'Mocha');


/*
NOTES:
before run please start mqtt:
  docker run -it -p 1883:1883 -p 9001:9001 eclipse-mosquitto  
*/

describe('EventStore', function () {
    // describe('Prepare EventStore', function () {
    //     it('instance EventStore with MQTT', function (done) {
    //         //ENVIRONMENT VARS
    //         const brokerUrl = 'mqtt://localhost:1883';
    //         const projectId = 'test';
    //         const eventsTopic = 'events-store-test';
    //         eventStore = new EventStore(
    //             {
    //                 type: "MQTT",
    //                 eventsTopic,
    //                 brokerUrl,
    //                 projectId,
    //             },
    //             {
    //                 type: 'MONGO',
    //                 connString: 'xxxx'
    //             }
    //         );
    //         assert.ok(true, 'EventStore constructor worked');
    //         return done();
    //     });
    // });
    // describe('Publish', function () {
    //     it('Publish event', function (done) {
    //         let event = new Event('Test', 1, 'TestCreated', { id: 1, name: 'x' }, 'Mocha');
    //         eventStore.emitEvent(event)
    //             .then(result => {
    //                 assert.ok(true, 'Event sent');
    //                 return done();
    //             }).catch(error => {
    //                 return done(error);
    //             });
    //     });
    // });
    // describe('de-prepare Event Store', function () {
    //     it('stop EventStore broker', function (done) {
    //         eventStore.broker.stopListening();
    //         assert.ok(true, 'Broker stoped');
    //         return done();
    //     });
    // });
});
