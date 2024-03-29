'use strict';

const { of, Observable, defer, bindNodeCallback, from, range } = require('rxjs');
const {
  mergeMap,
  mapTo,
  switchMap,
  pluck,
  map,
  concatMap,
  concatAll,
  filter,
  take,
  toArray,
  last,
  groupBy,
  bufferCount
} = require('rxjs/operators');
const MongoClient = require('mongodb').MongoClient;
const { Long } = require('mongodb');
const Event = require('../entities/Event');
const RETRIEVE_UNACK_CURSOR_SLEEP_MILLIS = parseInt(process.env.RETRIEVE_UNACK_CURSOR_SLEEP_MILLIS || '10');
const EVENT_STORE_STORE_MAX_INSERTS_COUNT = parseInt(process.env.EVENT_STORE_STORE_MAX_INSERTS_COUNT || '500');
const EVENT_STORE_STORE_WRITE_TIMEOUT = parseInt(process.env.EVENT_STORE_STORE_WRITE_TIMEOUT || '10000');

class MongoStore {
  constructor({ url, eventStoreDbName, aggregatesDbName }) {
    this.url = url;
    this.eventStoreDbName = eventStoreDbName;
    this.aggregatesDbName = aggregatesDbName;
  }

  /**
   * 
   * Starts DB connections
   * Returns an Obserable that resolves to each coneection result
   */
  start$() {
    const clientInstance = new MongoClient(this.url, {
      monitorCommands: true,
      serverSelectionTimeoutMS: 1000,
      useUnifiedTopology: true
    });

    return defer(() => clientInstance.connect()).pipe(
      map(client => {
        this.mongoClient = client;
        this.aggregatesDb = this.mongoClient.db(this.aggregatesDbName);
        client.on("serverHeartbeatFailed", (e) => {
          client.db("admin").collection('system.version').findOne({}).then((r) => {
            console.log(r);
            process.exit(1);
          });


        });
        return `MongoDB connected to dbName= ${this.dbName}`;
      }
      )
    );
  }

  /**
   * stops DB connections
   * returns an observable that resolves to text result of each closing db
   */
  stop$() {
    return Observable.create(observer => {
      this.mongoClient.close();
      observer.next('Mongo DB client closed');
      observer.complete();
    });
  }

  /**
   * Push an event (or array of events) into the store
   * Returns an observable that resolves to the same given input   
   * @param {Event} event event (or array of events) to store
   */
  pushEvent$(event) {

    if (!Array.isArray(event)) {
      const idAsBigInt = BigInt(event.id);
      event._id = new Long(Number(idAsBigInt & 0xFFFFFFFFn), Number((idAsBigInt >> 32n) & 0xFFFFFFFFn));
      const versionTimeStr = new Date(event.timestamp).getFullYear() + ('0' + (new Date(event.timestamp).getMonth() + 1)).slice(-2) + ('0' + (new Date(event.timestamp).getDate())).slice(-2);
      const eventStoreDb = this.mongoClient.db(`${this.eventStoreDbName}_${versionTimeStr.slice(0, 6)}`);
      const collection = eventStoreDb.collection(`Events_${versionTimeStr}`);
      return defer(() => collection.insertOne(event, { writeConcern: { w: 0, wtimeout: EVENT_STORE_STORE_WRITE_TIMEOUT, j: false } })).pipe(
        mapTo(event)
      );
    }

    return from(event).pipe(
      map(evt => ({ evt, at: evt.at, versionTimeStr: new Date(evt.timestamp).getFullYear() + ('0' + (new Date(evt.timestamp).getMonth() + 1)).slice(-2) + ('0' + (new Date(evt.timestamp).getDate())).slice(-2) })),
      groupBy(({ at, versionTimeStr }) => versionTimeStr),
      mergeMap(group => group.pipe(
        map(({ evt }) => {
          const idAsBigInt = BigInt(evt.id);
          evt._id = new Long(Number(idAsBigInt & 0xFFFFFFFFn), Number((idAsBigInt >> 32n) & 0xFFFFFFFFn));
          return evt;
        }),
        bufferCount(EVENT_STORE_STORE_MAX_INSERTS_COUNT),
        mergeMap(evts => {
          const versionTimeStr = group.key;
          return defer(() =>
            this.mongoClient
              .db(`${this.eventStoreDbName}_${versionTimeStr.slice(0, 6)}`)// per month DB
              .collection(`Events_${versionTimeStr}`) // per day Collection
              .insertMany(evts, { writeConcern: { w: 0, wtimeout: EVENT_STORE_STORE_WRITE_TIMEOUT, j: false } }));
        })
      )),
      last(),
      mapTo(event)
    );
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
    const versionTimeStr = versionDate.getFullYear() + ('0' + (versionDate.getMonth() + 1)).slice(-2);

    return this.getAggreate$(type, id, true, versionTime).pipe(
      switchMap(findResult => {
        const index =
          findResult.index && findResult.index[versionTimeStr]
            ? findResult.index[versionTimeStr]
            : {
              initVersion: findResult.version ? findResult.version + 1 : 1,
              initTime: findResult.versionTime
            };
        index.endVersion = findResult.version + 1;
        index.endTime = findResult.versionTime;

        const update = {
          $inc: { version: 1 },
          $set: {
            versionTime
          }
        };
        update.$set[`index.${versionTimeStr}`] = index;
        return bindNodeCallback(collection.findOneAndUpdate.bind(collection))({ type, id }, update, {
          upsert: true,
          returnOriginal: false
        });

      }),
      pluck('value'),
      map(aggregate => [aggregate, versionTimeStr])
    );
  }

  /**
   * Query an Aggregate in the store
   * Returns an observable that resolve to the Aggregate
   * @param {string} type
   * @param {string} id
   * @param {boolean} createIfNotExists if true, creates the aggregate if not found
   * @param {number} versionTime create time to set, ONLY FOR TESTING
   */
  getAggreate$(type, id, createIfNotExists = false, versionTime = Date.now()) {
    // Get the documents collection
    const collection = this.aggregatesDb.collection('Aggregates');
    return bindNodeCallback(collection.findOneAndUpdate.bind(collection))(
      {
        type,
        id
      },
      {
        $setOnInsert: {
          creationTime: versionTime
        }
      },
      {
        upsert: createIfNotExists,
        returnOriginal: false
      }
    ).pipe(map(result => (result && result.value ? result.value : undefined)));
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
    return this.getAggreate$(aggregateType, aggregateId).pipe(
      map(aggregate => {
        if (!aggregate) {
          throw new Error(
            `Aggregate not found: aggregateType=${aggregateType}  aggregateId=${aggregateId}`
          );
        }
        return aggregate;
      }),
      switchMap(aggregate =>
        from(Object.entries(aggregate.index)).pipe(
          filter(([time, index]) => minVersion <= index.endVersion),
          //.do(([time, index]) => console.log(`======== selected time frame: ${time}`))
          map(([time, index]) => {
            const eventStoreDb = this.mongoClient.db(
              `${this.eventStoreDbName}_${time}`
            );
            const collection = eventStoreDb.collection('Events');
            const lowLimit =
              minVersion > index.initVersion ? minVersion : index.initVersion;
            const highLimit =
              maxVersion < index.endVersion ? maxVersion : index.endVersion;
            const realLimit = highLimit - lowLimit + 1;
            //console.log(`========== ${time}: lowLimit=${lowLimit} highLimit=${highLimit}  realLimit=${realLimit} `);
            return bindNodeCallback(collection.find.bind(collection))({
              at: aggregateType,
              aid: aggregateId,
              av: { $gt: version }
            }).pipe(
              concatMap(cursor =>
                range(lowLimit, realLimit).pipe(mapTo(cursor))
              ),
              concatMap(cursor =>
                defer(() => this.extractNextFromMongoCursor(cursor))
              )
            );
          }),
          concatAll(),
          //.do(data => console.log(`============ ${data ? data.av : 'null'}`))
          filter(data => data),
          take(limit)
        )
      )
    );
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


    //Observable that resolves to the databases dates available from the eventstore
    const findAllDatabases$ = defer(() =>
      this.aggregatesDb.admin().listDatabases()
    ).pipe(
      mergeMap(response => from(response.databases)),
      pluck('name'),
      filter(dbName => dbName.indexOf(this.eventStoreDbName) !== -1),
      map(dbName => dbName.replace(`${this.eventStoreDbName}_`, '')),
      map(dbName => parseInt(dbName)),
      toArray(),
      map(arr => arr.sort()),
      mergeMap(array => from(array))
    );

    return this.findLatestAcknowledgedTimestamp$(aggregateType, key).pipe(
      // get latest ack timestamp
      mergeMap(latestAckTimeStamp => {
        const date = new Date(latestAckTimeStamp);
        const strDate =
          date.getFullYear() + ('0' + (date.getMonth() + 1)).slice(-2);
        const intDate = parseInt(strDate);
        // Find all DATABASES that hava events after the latest ack timestamp, with ASC order
        // resolving to the Events collection of each DB
        return findAllDatabases$.pipe(
          filter(dbDate => dbDate >= intDate),
          map(dbDate =>
            this.mongoClient.db(`${this.eventStoreDbName}_${dbDate}`)
          ),
          map(db => db.collection('Events')),
          map(evtCollection => {
            return { evtCollection, latestAckTimeStamp };
          })
        );
      }),
      map(({ evtCollection, latestAckTimeStamp }) => {
        //Trasform the collection observable to an Stream of Observables, each of these Observables will iterate over the collection and emmiting each event in order
        return of(
          evtCollection.find({
            at: aggregateType,
            timestamp: { $gt: latestAckTimeStamp }
          })
        ).pipe(mergeMap(cursor => this.extractAllFromMongoCursor$(cursor)));
      }),
      concatAll()
    ); // using concat we can asure to iterate over each database only if all the records on the database are exhausted.  this can be seen as a sync forEach
  }

  /**
   * Observable that resolves to latest timestamp Acknowledged for the given key
   * @param {*} findSearchQuery
   * @param {*} findSearchProjection
   */
  findLatestAcknowledgedTimestamp$(aggregateType, key) {
    //FIND document queries
    const findSearchQuery = { at: aggregateType, key };
    //lets build all const queries so the RxJS stream is more legible
    return defer(() =>
      this.aggregatesDb
        .collection('Acknowledges')
        .findOne(findSearchQuery)
    ).pipe(
      map(findResult => {
        return findResult // checks if document found
          ? findResult.ts // if found resolves to latest acknowledged timestamp
          : 0; // if not found resolves to zero as timestamp
      })
    );
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
    return Observable.create(async observer => {
      const collection = this.aggregatesDb.collection('Aggregates');
      const cursor = collection.find({
        creationTime: { $gt: createTimestamp },
        type: type
      });
      let obj = await this.extractNextFromMongoCursor(cursor);
      while (obj) {
        observer.next(obj);
        obj = await this.extractNextFromMongoCursor(cursor);
      }

      observer.complete();
    });
  }

  /**
   * Ensure the existence of a registry on the ack database for an aggregate/key pair
   * @param {string} aggregateType aggregate type
   * @param {string} key backend key
   */
  ensureAcknowledgeRegistry$(aggregateType, key) {
    //lets build all const queries so the RxJS stream is more legible
    // collection to use
    const collection = this.aggregatesDb.collection('Acknowledges');

    //UPDATE KEY queries
    const updateSearchQuery = {
      at: aggregateType,
      key,
    };
    const updateQuery = {
      $setOnInsert: { ts: 0 },
      $set: { ets: Date.now() }
    };

    const writeConcern = { w: 1, wtimeout: 2000, j: true };
    const updateOps = {
      upsert: true,
      writeConcern
    };

    return defer(() =>
      collection.updateOne(updateSearchQuery, updateQuery, updateOps)
    ).pipe(
      map(updateResult => `ensured Acknowledge Registry: ${JSON.stringify({ aggregateType, key })}`)
    );
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
      at: event.at,
      key,
      ts: { $lt: event.timestamp }
    };
    const updateQuery = {
      $set: { ts: event.timestamp }
    };

    const writeConcern = { w: 0, wtimeout: 2000, j: false };
    const updateOps = {
      upsert: false,
      writeConcern
    };

    return defer(() =>
      collection.updateOne(updateSearchQuery, updateQuery, updateOps)
    ).pipe(
      map(updateResult => event)
    );
  }

  /**
   * extracts every item in the mongo cursor, one by one
   * @param {*} cursor
   */
  extractAllFromMongoCursor$(cursor) {
    return Observable.create(async observer => {
      let obj = await this.extractNextFromMongoCursor(cursor);
      while (obj) {
        observer.next(obj);
        await this.sleep(RETRIEVE_UNACK_CURSOR_SLEEP_MILLIS);
        obj = await this.extractNextFromMongoCursor(cursor);
      }
      observer.complete();
    });
  }

  sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
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
