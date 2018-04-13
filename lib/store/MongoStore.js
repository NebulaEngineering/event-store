'use strict'

const Rx = require('rxjs');
const MongoClient = require('mongodb').MongoClient;
const RetrieveEventResult = require('../entities/RetrieveEventResult');
const RetrieveNewAggregateResult = require('../entities/RetrieveNewAggregateResult');
const Event = require('../entities/Event');

class MongoStore {

    constructor({ url, eventStoreDbName, aggregatesDbName }) {
        this.url = url;
        this.eventStoreDbName = eventStoreDbName;
        this.aggregatesDbName = aggregatesDbName;
    }

    /**
     * Starts DB connections
     * Returns an Obserable that resolves to each coneection result
     */
    init$() {
        return Rx.Observable.bindNodeCallback(MongoClient.connect)(this.url)
            .map(client => {
                this.mongoClient = client;
                this.aggregatesDb = this.mongoClient.db(this.aggregatesDbName);
                return `MongoStore DB connected`;
            });
    }



    /**
     * Push an event into the store
     * Returns an observable that resolves to {aggregate,event,versionTimeStr}
     * where:
     *  - aggregate = current aggregate state
     *  - event = persisted event
     *  - versionTimeStr = EventStore date index where the event was store
     * 
     * @param {Event} event 
     */
    pushEvent$(event) {
        if (!event.timestamp) {
            event.timestamp = Date.now();
        }
        return this.incrementAggregateVersionAndGet$(event.at, event.aid, event.timestamp)
            .mergeMap(([aggregate, versionTimeStr]) => {
                event.av = aggregate.version;
                const eventStoreDb = this.mongoClient.db(`${this.eventStoreDbName}_${versionTimeStr}`);
                const collection = eventStoreDb.collection('Events');
                return Rx.Observable.fromPromise(collection.insertOne(
                    event,
                    { writeConcern: { w : "majority", wtimeout : 200, j: true } }
                ))
                    .mapTo({ aggregate, event, versionTimeStr });
            })
            ;
    }

    /**
     * Increments the aggregate version and return the aggregate itself
     * Returns an observable that resolve to the an array: [Aggregate, TimeString]
     * the TimeString is the name postfix of the EventStore DB where this aggregate version must be persisted
     * @param {string} type 
     * @param {string} id 
     * @param {number} versionTime 
     */
    incrementAggregateVersionAndGet$(type, id, versionTime) {
        // Get the documents collection        
        const collection = this.aggregatesDb.collection('aggregates');
        //if the versionTime is not provided (production), then we generate with the current date time
        if (!versionTime) {
            versionTime = Date.now();
        }
        const versionDate = new Date(versionTime);
        const versionTimeStr = versionDate.getFullYear() + ("0" + (versionDate.getMonth() + 1)).slice(-2);

        return this.getAggreate$(type, id, true, versionTime)
            .switchMap(findResult => {
                const index = findResult.index && findResult.index[versionTimeStr] ? findResult.index[versionTimeStr] : { initVersion: findResult.version ? findResult.version + 1 : 1 , initTime: findResult.versionTime };
                index.endVersion = findResult.version + 1;
                index.endTime = findResult.versionTime;

                const update = {
                    $inc: { version: 1 },
                    $set: {
                        versionTime,
                    }
                };
                update['$set'][`index.${versionTimeStr}`] = index;
                return Rx.Observable.bindNodeCallback(collection.findOneAndUpdate.bind(collection))(
                    { type, id },
                    update,
                    {
                        upsert: true,
                        returnOriginal: false
                    });
            })
            .pluck('value')
            .map(aggregate => [aggregate, versionTimeStr])
            ;
    }


    /**
     * Query an Aggregate in the store 
     * Returns an observable that resolve to the Aggregate 
     * @param {string} type 
     * @param {string} id 
     * @param {boolean} createIfNotExists if true, creates the aggregate if not found
     * @param {number} versionTime create time to set, ONLY FOR TESTING
     */
    getAggreate$(type, id, createIfNotExists = false, versionTime) {
        //if the versionTime is not provided (production), then we generate with the current date time
        if (!versionTime) {
            versionTime = Date.now();
        }
        // Get the documents collection        
        const collection = this.aggregatesDb.collection('aggregates');
        return Rx.Observable.bindNodeCallback(collection.findOneAndUpdate.bind(collection))(
            {
                type, id
            },
            {
                $setOnInsert: {
                    creationTime: versionTime,
                    versionTime,
                    //version: 0
                },
            },
            {
                upsert: createIfNotExists,
                returnOriginal: false
            }
        )
            .map(result => result && result.value ? result.value : undefined)
            ;
    }


    /**
     * Find all events of an especific aggregate
     * @param {String} aggregateType Aggregate type
     * @param {String} aggregateId Aggregate Id
     * @param {number} version version to recover from (exclusive), defualt = 0
     * @param {limit} limit max number of events to return, default = 20
     * 
     */
    getEvents$(aggregateType, aggregateId, version = 0, limit = 20) {
        const minVersion = version + 1;
        const maxVersion = version + limit;        
        //console.log(`====== getEvents$: minVersion=${minVersion}, maxVersion=${maxVersion}`);
        return this.getAggreate$(aggregateType, aggregateId)
            .map(aggregate => {
                if (!aggregate) {
                    throw new Error(`Aggregate not found: aggregateType=${aggregateType}  aggregateId=${aggregateId}`);
                }
                return aggregate;
            })
            .switchMap(aggregate =>
                Rx.Observable.from(Object.entries(aggregate.index))
                    .filter(([time, index]) => minVersion <= index.endVersion)
                    .do(([time, index]) => console.log(`======== selected time frame: ${time}`))
                    .map(([time, index]) => {
                        const eventStoreDb = this.mongoClient.db(`${this.eventStoreDbName}_${time}`);
                        const collection = eventStoreDb.collection('Events');
                        const lowLimit = minVersion > index.initVersion ? minVersion : index.initVersion;
                        const highLimit = maxVersion < index.endVersion ? maxVersion : index.endVersion;
                        const realLimit = highLimit - lowLimit + 1;
                        //console.log(`========== ${time}: lowLimit=${lowLimit} highLimit=${highLimit}  realLimit=${realLimit} `);
                        return Rx.Observable.bindNodeCallback(collection.find.bind(collection))({
                            at: aggregateType,
                            aid: aggregateId,
                            av: { $gte: version }
                        })
                            .concatMap(cursor => Rx.Observable.range(lowLimit, realLimit).mapTo(cursor))
                            .concatMap(cursor => Rx.Observable.fromPromise(this.extractNextFromMongoCursor(cursor)))
                    })
                    .concatAll()
                    //.do(data => console.log(`============ ${data ? data.av : 'null'}`))
                    .filter(data => data)                    
                    .take(limit)
            )
    }

    async extractNextFromMongoCursor(cursor) {
        const hasNext = await cursor.hasNext();
        //console.log(`#######################  ${hasNext}`);        
        if(hasNext){
            const  obj = await cursor.next();
            //console.log(`**********************  ${hasNext}:  ${obj}`);
            return  obj;
        }
        return undefined;       
    }

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
            this.mongoClient.close();
            observer.next('Mongo DB client closed');
            observer.complete();
        });
    }
}

module.exports = MongoStore;