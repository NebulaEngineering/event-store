'use strict'

const Rx = require('rxjs');
const MongoClient = require('mongodb').MongoClient;

class MongoStore {

    constructor(storeConfig) {
        const that = this;    
        MongoClient.connect(storeConfig.connString, function(err, client) {
            if(err){
                console.log(`Error connecting Mongo DB: ${err}`);
            }else{
                console.log(`Connected successfully to server ${storeConfig.connString}`); 
                that.dbClient = client;
            }                      
        });
    }

    /**
     * Find Aggragates events and returns a promise of RetrieveEventResult
     * @param {string} aggregateType 
     * @param {string} aggregateId 
     * @param {number} aggregateVersion 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findEvents(aggregateType, aggregateId, aggregateVersion, {offset,pageSize}){
        
    }

    /**
     * Find Aggragates that were created after the given date and returns a promise of RetrieveNewAggregateResult
     * @param {string} aggregateType 
     * @param {number} createTimestamp 
     * @param {Object} ops {offset,pageSize}
     * 
     */
    findAgregatesCreatedAfter(aggregateType, createTimestamp,{offset,pageSize}){
        return Rx.Observable.of(0)
            .map(() => {
                // that.dbClient.db(aggregateType).collection('agregates')
                
                // .publish(`${this.projectId}/${this.topicName}`, dataBuffer, { qos: 1 });
                // return uuid;
            });
    }

    closeConnection(){
        that.dbClient.close()
    }
}

module.exports = MongoStore;