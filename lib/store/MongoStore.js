'use strict'

const Rx = require('rxjs');
const MongoClient = require('mongodb').MongoClient;
const RetrieveEventResult = require('../entities/RetrieveEventResult');
const RetrieveNewAggregateResult = require('../entities/RetrieveNewAggregateResult');

class MongoStore {

    constructor(storeConfig) {
        //this.that = this;
        const that = this;
        MongoClient.connect(storeConfig.connString, function (err, client) {
            if (err) {
                console.log(`Error connecting Mongo DB: ${err}`);
            } else {
                console.log(`Connected successfully to server ${storeConfig.connString}`);
                that.dbClient = client;
            }
        });
    }

    /**
     * Find Aggregates events and returns a promise of RetrieveEventResult
     * @param {string} aggregateType 
     * @param {string} aggregateId 
     * @param {number} aggregateVersion 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findEvents$(aggregateType, aggregateId, aggregateVersion, { pageSize }) {
        const that = this;
        that.findAgregate$(aggregateType, aggregateVersion).map(aggregate => {
            Rx.Observable.from(aggregate.index)
                .filter(index => start >= aggregateVersion && end <= aggregateVersion)
                .map(index => {
                    return this.dbClient
                        .db('event-store-db')
                        .collection(`events-${index.year}-${index.month}`)
                        .find(
                            { aggregateType: { $eq: aggregateType }, aggregateId: { $eq: aggregateId } }, //Query
                    )
                        .limit(pageSize)
                        .toArray()
                })
        })
    }

    /**
     * Find an aggregate according to the type and the version
     * @param {*} aggregateType 
     * @param {*} aggregateVersion 
     */
    findAgregate$(aggregateType, aggregateVersion) {
        const that = this;

        return Rx.Observable.fromPromise(
            that.dbClient
                .db('event-store-db')
                .collection('aggregates')
                .find({ aggregateType: { $eq: aggregateType }, aggregateVersion: { $eq: aggregateVersion } })
                .limit(pageSize)
                .skip(offset).toArray());
    }

    /**
     * Find Aggregates that were created after the given date and returns a promise of RetrieveNewAggregateResult
     * @param {string} aggregateType 
     * @param {number} createTimestamp 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findAgregatesCreatedAfter$(aggregateType, createTimestamp, { offset, pageSize }) {

        query = this.dbClient.db('event-store-db')
            .collection('aggregates')
            .find({ creationTime: { $gt: createTimestamp }, aggregateType: aggregateType }, //Query
                { aggregateId: 1 }) //Projection
            .limit(pageSize)
            .skip(offset);

        return Rx.Observable.bindNodeCallback(query.toArray)()
            .mergeMap(docsArray => Rx.Observable.from(docsArray))
            .pluck('aggregateId')
            .reduce((acc, aggregateId) => {
                acc.push(aggregateId);
                return acc;
            }, [])
            .map(idArray => new RetrieveNewAggregateResult(idArray, { moreResults: idArray.length == pageSize }));
    }

    closeConnection() {
        this.dbClient.close();
    }
}

module.exports = MongoStore;