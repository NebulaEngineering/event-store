// TEST LIBS
const assert = require('assert');
const Rx = require('rxjs');
const should = require('chai').should();

//LIBS FOR TESTING
const MongoStore = require('../../lib/store/MongoStore');
const Event = require('../../lib/entities/Event');

//GLOABAL VARS to use between tests
let store = {};
let event = new Event('Test', 1, 'TestCreated', { id: 1, name: 'x' }, 'Mocha');


/*
NOTES:
before run please start mongo:
  docker run --rm  -d  -p 27017:27017  mongo --storageEngine wiredTiger
*/

describe('MONGO STORE', function () {
    describe('Prepare store', function () {
        it('instance MongoStore', function (done) {
            store = new MongoStore({ url: 'mongodb://localhost:27017', aggregatesDbName: 'Aggregates', eventStoreDbName: 'EventStore' });
            store.init$()
                .subscribe(
                    () => {
                    },
                    (error) => {
                        console.error(`Failed to connect MongoStore`);
                        return done(error);
                    },
                    () => {
                        console.log(`MongoStore connected`);
                        should.exist(store.eventStoreDb, 'eventStoreDb not defined');
                        should.exist(store.eventStoreDbClient, 'eventStoreDbClient not defined');
                        should.exist(store.aggregatesDb, 'aggregatesDb not defined');
                        should.exist(store.aggregatesDbClient, 'aggregatesDbClient not defined');
                        return done();
                    }
                );
        });
    });
    describe('Events storing', function () {
        it('increment and get aggregate version', function (done) {
            Rx.Observable.concat(
                store.incrementAggregateVersionAndGet$('SampleAggregate', '1234567890'),
                store.incrementAggregateVersionAndGet$('SampleAggregate', '1234567890'),
                store.incrementAggregateVersionAndGet$('SampleAggregate', '1234567890')
            ).subscribe(
                ([v1, v2, v3]) => {
                    assert(v2.version, v1.version + 1);
                    assert(v3.version, v2.version + 1);
                },
                (error) => {
                    console.log(`Error incrementing and getting Aggregate versions`,error);               
                    return done(error);
                },
                () => {
                    return done();
                }
            );
        });
    });



    // describe('Publish and listen on MQTT', function () {
    //     it('Publish event and recive my own event on MQTT', function (done) {
    //         this.timeout(10000);
    //         let event1 = new Event('TestCreated', 1, 'Test', 1, 1, { id: 1, name: 'x' }, 'Mocha');
    //         //let event1 = new Event('Test', 1, 'TestCreated', { id: 1, name: 'x' }, 'Mocha');
    //         mqttBroker.getEventListener$('Test', false)
    //             .first()
    //             //.timeout(1500)
    //             .subscribe(
    //                 (evt) => {
    //                     incomingEvent = evt;
    //                     //console.log('==============> Expected message -> ', event1.timestamp + " -- "+ evt.timestamp);
    //                     assert.deepEqual(evt, event1);
    //                 },
    //                 error => {
    //                     return done(new Error(error));
    //                 },
    //                 () => {
    //                     return done();
    //                 }
    //             );
    //         mqttBroker.publish$(event1).subscribe(
    //             () => { },
    //             (err) => console.error(err),
    //             () => { }
    //         );
    //     });
    //     it('Publish event and DO NOT recieve my own event on MQTT', function (done) {
    //         let event2 = new Event('TestCreated', 1, 'Test', 1, 1, { id: 1, name: 'x' }, 'Mocha');
    //         mqttBroker.getEventListener$('Test')
    //             .first()
    //             .timeout(500)
    //             .subscribe(
    //                 (evt) => {
    //                     incomingEvent = evt;
    //                     //console.log('==============> Unexpected message -> ', event2.timestamp + " -- "+ evt.timestamp);
    //                     assert.notDeepEqual(evt, event2);
    //                     //assert.fail(evt, 'nothing', 'Seems I have recieved the same evt I just sent');
    //                 },
    //                 error => {
    //                     assert.equal(error.name, 'TimeoutError');
    //                     return done();
    //                 },
    //                 () => {
    //                     return done();
    //                 }
    //             );
    //         mqttBroker.publish$(event2).subscribe(
    //             () => { },
    //             (err) => console.error(err),
    //             () => { }
    //         );
    //     });
    // });
    describe('de-prepare MongoStore', function () {
        it('stop MongoStore', function (done) {
            store.stop$()
                .subscribe(
                    next => console.log(next),
                    (error) => {
                        console.error(`Error stopping MongoStore`, error);
                        return done(error);
                    },
                    () => {
                        console.log('MongoStore stopped');
                        assert.ok(true, 'MqttBroker stoped');
                        return done();
                    }
                );
        });
    });
});
