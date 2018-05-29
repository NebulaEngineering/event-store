'use strict'

const Rx = require('rxjs');
const MongoClient = require('mongodb').MongoClient;
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
    start$() {
        return Rx.Observable.bindNodeCallback(MongoClient.connect)(this.url)
            .map(client => {
                this.mongoClient = client;
                this.aggregatesDb = this.mongoClient.db(this.aggregatesDbName);
                return `MongoStore DB connected`;
            });
    }

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
                return Rx.Observable.defer(() => collection.insertOne(
                    event,
                    { writeConcern: { w: "majority", wtimeout: 500, j: true } }
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
        const collection = this.aggregatesDb.collection('Aggregates');
        //if the versionTime is not provided (production), then we generate with the current date time
        if (!versionTime) {
            versionTime = Date.now();
        }
        const versionDate = new Date(versionTime);
        const versionTimeStr = versionDate.getFullYear() + ("0" + (versionDate.getMonth() + 1)).slice(-2);

        return this.getAggreate$(type, id, true, versionTime)
            .switchMap(findResult => {
                const index = findResult.index && findResult.index[versionTimeStr] ? findResult.index[versionTimeStr] : { initVersion: findResult.version ? findResult.version + 1 : 1, initTime: findResult.versionTime };
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
        const collection = this.aggregatesDb.collection('Aggregates');
        return Rx.Observable.bindNodeCallback(collection.findOneAndUpdate.bind(collection))(
            {
                type, id
            },
            {
                $setOnInsert: {
                    creationTime: versionTime,
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
     * Returns an Observable that emits each found event one by one
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
                    //.do(([time, index]) => console.log(`======== selected time frame: ${time}`))
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
                            av: { $gt: version }
                        })
                            .concatMap(cursor => Rx.Observable.range(lowLimit, realLimit).mapTo(cursor))
                            .concatMap(cursor => Rx.Observable.defer(() => this.extractNextFromMongoCursor(cursor)))
                    })
                    .concatAll()
                    //.do(data => console.log(`============ ${data ? data.av : 'null'}`))
                    .filter(data => data)
                    .take(limit)
            )
    }


    /**
     * Find all events of an especific aggregate having taken place but not acknowledged,
     * @param {String} aggregateType Aggregate type
     * @param {string} key process key (eg. microservice name) that acknowledged the events
     * 
     * Returns an Observable that emits each found event one by one
     */
    retrieveUnacknowledgedEvents$(aggregateType, key) {

        /*
            1 - retrieves the latest acknowledged event for the given aggregateType and key
                1.1 - if the key exists, extracts the timestamp
                1.2 - if the key does not exists, set the timestamp to zero
            2 - extract all the existing DBs for event storing, sort it by date, and filter after the timestamp extracted at (1)
            3 - iterates by every DB and emmits every event of the given Aggregate type after the timestamp extracted at (1)
        */

        //lets build all const queries so the RxJS stream is more legible
        // collection to use
        const ackCollection = this.aggregatesDb.collection('Acknowledges');
        //FIND document queries
        const findSearchQuery = { "at": aggregateType };
        findSearchQuery[`ack.${key}`] = { "$exists": true };
        const findSearchProjection = { "_id": 1, };
        findSearchProjection[`ack.${key}`] = 1;

        //Observable that resolves to latest timestamp Acknowledged for the given key
        const findLatestAcknowledgedTimestamp$ =
            Rx.Observable.defer(() => ackCollection.findOne(findSearchQuery, findSearchProjection))
                .map(findResult => {
                    return findResult // checks if document found
                        ? findResult.ack[key].ts // if found resolves to latest acknowledged timestamp
                        : 0;// if not found resolves to zero as timestamp 
                });

        //Observable that resolves to the databases dates available from the eventstore
        const findAllDatabases$ = Rx.Observable.defer(() => this.aggregatesDb.admin().listDatabases())
            .mergeMap(response => Rx.Observable.from(response.databases))
            .pluck('name')
            .filter(dbName => dbName.indexOf(this.eventStoreDbName) !== -1)
            .map(dbName => dbName.replace(`${this.eventStoreDbName}_`, ''))
            .map(dbName => parseInt(dbName))
            .toArray().map(arr => arr.sort())
            .mergeMap(array => Rx.Observable.from(array))
            ;

        return findLatestAcknowledgedTimestamp$ // get latest ack timestamp
            .mergeMap(latestAckTimeStamp => {
                const date = new Date(latestAckTimeStamp);
                const strDate = date.getFullYear() + ("0" + (date.getMonth() + 1)).slice(-2);
                const intDate = parseInt(strDate);
                // Find all DATABASES that hava events after the latest ack timestamp, with ASC order
                // resolving to the Events collection of each DB
                return findAllDatabases$
                    .filter(dbDate => dbDate >= intDate)
                    .map(dbDate => this.mongoClient.db(`${this.eventStoreDbName}_${dbDate}`))
                    .map(db => db.collection('Events'))
                    .map(evtCollection => { return { evtCollection, latestAckTimeStamp } });
            })
            .map(({ evtCollection, latestAckTimeStamp }) => {
                //Trasform the collection observable to an Stream of Observables, each of these Observables will iterate over the collection and emmiting each event in order                
                return Rx.Observable.of(evtCollection.find({ at: aggregateType, timestamp: { "$gt": latestAckTimeStamp } }))
                    .mergeMap(cursor => this.extractAllFromMongoCursor$(cursor))
            })
            .concatAll(); // using concat we can asure to iterate over each database only if all the records on the database are exhausted.  this can be seen as a sync forEach            
    }


    /**
     * Find Aggregates that were created after the given date
     * Returns an observable that publish every aggregate found
     * @param {string} type 
     * @param {number} createTimestamp 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findAgregatesCreatedAfter$(type, createTimestamp = 0) {
        return Rx.Observable.create(async observer => {
            const collection = this.aggregatesDb.collection('Aggregates');
            const cursor = collection.find({ creationTime: { $gt: createTimestamp }, type: type });
            let obj = await this.extractNextFromMongoCursor(cursor);
            while (obj) {
                observer.next(obj);
                obj = await this.extractNextFromMongoCursor(cursor);
            }

            observer.complete();
        });
    }

    /**
     * Ensure the existence of a registry on the ack database for an aggregate type
     * @param {string} aggregateType 
     */
    ensureAcknowledgeRegistry$(aggregateType) {
        /*
           tries to create a registry for the aggregateType if not exists
        */

        //lets build all const queries so the RxJS stream is more legible
        // collection to use
        const collection = this.aggregatesDb.collection('Acknowledges');

        //UPDATE KEY queries
        const updateSearchQuery = {
            "at": aggregateType,
        };

        const updateQuery = {
            "$set": {
                "at": aggregateType,
            },
            "$setOnInsert": { "ack": {} }
        };


        const writeConcern = { w: "majority", wtimeout: 500, j: true };
        const updateOps = {
            upsert: true,
            writeConcern
        };

        return Rx.Observable.defer(() => collection.updateOne(updateSearchQuery, updateQuery, updateOps))
            .mergeMap(updateResult => {
                return (updateResult.upsertedCount > 0) // checks if the insert was made
                    ? Rx.Observable.of(`EventStore has created an empty registry for ${aggregateType}`)
                    : Rx.Observable.of(`EventStore has verified the existence of a registry for ${aggregateType}`);
            });
    }

    /**
     * persist the event acknowledge
     * return an observable that resolves to the same given event
     * @param {Event} event event to acknowledge 
     * @param {string} key process key (eg. microservice name) that is acknowledging the event
     */
    acknowledgeEvent$(event, key) {

        /*
           tries to update the latest Acknowledged event if the aggregate version is higher than the current version
            if no modification was made is because:
              a) the given event aggregate version is lower or equals to the current version.  in this case no oparation is done
              b) the record did no exists.  in this case the record will be created
        */

        //lets build all const queries so the RxJS stream is more legible
        // collection to use
        const collection = this.aggregatesDb.collection('Acknowledges');

        //UPDATE KEY queries
        const updateSearchQuery = {
            "at": event.at,
        };
        updateSearchQuery[`ack.${key}.ts`] = { "$lt": event.timestamp };
        const updateQuery = {
            "$set": {
            }
        };
        updateQuery["$set"][`ack.${key}.ts`] = event.timestamp;

        const writeConcern = { w: "majority", wtimeout: 500, j: true };
        const updateOps = {
            upsert: false,
            writeConcern
        };

        //FIND document queries
        const findSearchQuery = { "at": event.at };
        const findSearchProjection = { "_id": true, };
        findSearchProjection[`ack.${key}`] = true;

        //INSERT new document queries
        const insertEntireDocumentQuery = {
            "at": event.at,
            "ack": {}
        };
        insertEntireDocumentQuery.ack[key] = { "ts": event.timestamp };


        return Rx.Observable.defer(() => collection.updateOne(updateSearchQuery, updateQuery, updateOps))
            .mergeMap(updateResult => {
                return (updateResult.nModified > 0) // checks if the update was made
                    ? Rx.Observable.of(event) // if updated then returns the event 
                    : (updateResult.n > 0) // checks if the document was found even if it was not updated
                        ? Rx.Observable.throw(new Error('Error while persisting event acknowledge, the document was found but no modification was done')) // if the document was found but not modified then is an error
                        : Rx.Observable.defer(() => collection.findOne(findSearchQuery, findSearchProjection)) // attemps to retrieve the document to check if it does exists or is a version ahead
                            .mergeMap(findResult => {
                                return (!findResult) // chechks if the document was not found
                                    ? Rx.Observable.defer(() => collection.insertOne(insertEntireDocumentQuery, writeConcern)) // the document does not exists, so it can be created as new
                                        .mapTo(event) //returns the event 
                                    : (findResult.ack[key]) // the document does exists, lets chek if key exists
                                        ? Rx.Observable.of(event) // the key exists, so no need to create it.  just return the event
                                        : Rx.Observable.defer(() => collection.updateOne(findSearchQuery, updateQuery, updateOps)) // the key does not exists, so lets update the documento so the key is created
                                            .mapTo(event); //returns the event 
                            });
            });
    }

    /**
     * extracts every item in the mongo cursor, one by one
     * @param {*} cursor 
     */
    extractAllFromMongoCursor$(cursor) {
        return Rx.Observable.create(async observer => {
            let obj = await this.extractNextFromMongoCursor(cursor);
            while (obj) {
                observer.next(obj);
                obj = await this.extractNextFromMongoCursor(cursor);
            }
            observer.complete();
        });
    }

    /**
     * Extracts the next value from a mongo cursos if available, returns undefined otherwise
     * @param {*} cursor 
     */
    async extractNextFromMongoCursor(cursor) {
        const hasNext = await cursor.hasNext();
        if (hasNext) {
            const obj = await cursor.next();
            return obj;
        }
        return undefined;
    }

}

module.exports = MongoStore;