'use strict'

const Rx = require('rxjs');
const MongoClient = require('mongodb').MongoClient;
const RetrieveEventResult = require('../entities/RetrieveEventResult');
const RetrieveNewAggregateResult = require('../entities/RetrieveNewAggregateResult');

class MongoStore {

    constructor({ url, eventStoreDbName, aggregatesDbName }) {
        this.url = url;
        this.eventStoreDbName = this.eventStoreDbName;
        this.aggregatesDbName = this.aggregatesDbName;
    }

    /**
     * Starts DB connections
     * Returns an Obserable that resolves to each coneection result
     */
    init$() {
        return Rx.Observable.forkJoin(
            Rx.Observable.bindNodeCallback(MongoClient.connect)(this.url)
                .map(client => {
                    this.eventStoreDbClient = client;
                    this.eventStoreDb = client.db(this.eventStoreDbName);
                    return `MongoStore DB ${this.eventStoreDbName} connected`;
                }),
            Rx.Observable.bindNodeCallback(MongoClient.connect)(this.url)
                .map(client => {
                    this.aggregatesDbClient = client;
                    this.aggregatesDb = client.db(this.aggregatesDbName);
                    return `MongoStore DB ${this.aggregatesDbName} connected`;
                }))
            ;
    }

    /**
     * Push an event into the store
     * @param {Event} event 
     */
    pushEvent$(event) {

    }

    /**
     * Increments the aggregate version and return the aggregate itself
     * 
     * @param {string} type 
     * @param {string} id 
     * @param {number} versionTime 
     */
    incrementAggregateVersionAndGet$(type, id, versionTime) {
        // Get the documents collection        
        const collection = this.aggregatesDb.collection('aggregates');

        if (!versionTime) {
            versionTime = Date.now();
        }


         return Rx.Observable.bindNodeCallback(collection.findOneAndUpdate)(
            
                {
                    type, id
                },
                {
                    $setOnInsert: { versionTime },
                    $inc: { version: 1 },
                    $set: {
                        versionTime,
                    }
                },
                {
                    upsert: true,
                    returnOriginal: false
                }
            )
            .switchMap(updateResult => {
                console.log("###############");
                console.log("###############");
                console.log(`UPDATE RESULT= ${JSON.stringify(updateResult)}`);
                const versionDate = new Date(updateResult.versionTime);
                const versionTimeStr = versionDate.getFullYear() + ("0" + versionDate.getMonth()).slice(-2);
                const index = updateResult.index && updateResult.index[versionTimeStr] ? updateResult.index[versionTimeStr] : { initVersion: updateResult.version, initTime: updateResult.versionTime };
                index.endVersion = updateResult.version;
                index.endTime = updateResult.versionTime;

                const update = {
                    '$set': {}
                };
                update['$set'][`index.${versionTimeStr}`] = index;
                return Rx.Observable.bindNodeCallback(collection.findOneAndUpdate)(
                    { type, id },
                    update,
                    {
                        upsert: true,
                        returnOriginal: false
                    })
            })
            ;
    }

    // /**
    //  * Find Aggregates events and returns a promise of RetrieveEventResult
    //  * @param {string} aggregateType 
    //  * @param {string} aggregateId 
    //  * @param {number} aggregateVersion 
    //  * @param {Object} ops {offset,pageSize}
    //  * 
    //  */
    // findEvents$(aggregateType, aggregateId, aggregateVersion, { pageSize }) {
    //     const that = this;
    //     that.findAgregate$(aggregateType, aggregateVersion).map(aggregate => {
    //         Rx.Observable.from(aggregate.index)
    //             .filter(index => start >= aggregateVersion && end <= aggregateVersion)
    //             .map(index => {
    //                 return this.dbClient
    //                     .db('event-store-db')
    //                     .collection(`events-${index.year}-${index.month}`)
    //                     .find(
    //                         { aggregateType: { $eq: aggregateType }, aggregateId: { $eq: aggregateId } }, //Query
    //                 )
    //                     .limit(pageSize)
    //                     .toArray()
    //             })
    //     })
    // }

    // /**
    //  * Find an aggregate according to the type and the version
    //  * @param {*} aggregateType 
    //  * @param {*} aggregateVersion 
    //  */
    // findAgregate$(aggregateType, aggregateVersion) {
    //     const that = this;

    //     return Rx.Observable.fromPromise(
    //         that.dbClient
    //             .db('event-store-db')
    //             .collection('aggregates')
    //             .find({ aggregateType: { $eq: aggregateType }, aggregateVersion: { $eq: aggregateVersion } })
    //             .limit(pageSize)
    //             .skip(offset).toArray());
    // }

    // /**
    //  * Find Aggregates that were created after the given date and returns a promise of RetrieveNewAggregateResult
    //  * @param {string} aggregateType 
    //  * @param {number} createTimestamp 
    //  * @param {Object} ops {offset,pageSize}
    //  * 
    //  */
    // findAgregatesCreatedAfter$(aggregateType, createTimestamp, { offset, pageSize }) {

    //     query = this.dbClient.db('event-store-db')
    //         .collection('aggregates')
    //         .find({ creationTime: { $gt: createTimestamp }, aggregateType: aggregateType }, //Query
    //             { aggregateId: 1 }) //Projection
    //         .limit(pageSize)
    //         .skip(offset);

    //     return Rx.Observable.bindNodeCallback(query.toArray)()
    //         .mergeMap(docsArray => Rx.Observable.from(docsArray))
    //         .pluck('aggregateId')
    //         .reduce((acc, aggregateId) => {
    //             acc.push(aggregateId);
    //             return acc;
    //         }, [])
    //         .map(idArray => new RetrieveNewAggregateResult(idArray, { moreResults: idArray.length == pageSize }));
    // }

    /**
     * stops DB connections
     * returns an observable that resolves to text result of each closing db
     */
    stop$() {
        return Rx.Observable.create(observer => {
            this.eventStoreDbClient.close();
            observer.next('EventStore DB closed');
            this.aggregatesDbClient.close();
            observer.next('Aggregates DB closed');
            observer.complete();
        });
    }
}

module.exports = MongoStore;