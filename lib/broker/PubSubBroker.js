'use strict';

const { Subject, BehaviorSubject, Observable, defer, from, of, timer, interval, throwError } = require('rxjs');
const { filter, map, tap, mergeMap, last, mapTo, bufferTime, retryWhen, delayWhen, switchMap, takeUntil, catchError, first, timeout } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub, v1 } = require('@google-cloud/pubsub');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const googleCredentials = require(process.env.GOOGLE_APPLICATION_CREDENTIALS || "");

const MSG_STATES = {
    RECEIVED: 10,
    IGNORED: 20,
    IGNORED_BPAR: 21,
    IGNORED_PPF: 22,
    IGNORED_APAR: 22,
    IGNORED_SELF: 23,
    IGNORED_SENT: 24,
    AUTO_ACK: 30,
    PROCESS_READY: 41,
    PROCESS_SENT: 42,
    PROCESS_DONE: 43,
    ACK_ERROR: 100
};

const EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED = process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null ? (process.env.EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED == "true") : null;
const EVENT_STORE_BROKER_MAX_MESSAGES = parseInt(process.env.EVENT_STORE_BROKER_MAX_MESSAGES || "1500");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG || "1000");
const EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS = parseInt(process.env.EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS || "200");
const EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS = parseInt(process.env.EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS || "3", 10);
const EVENT_STORE_BROKER_ACK_TIME_BUFFER = parseInt(process.env.EVENT_STORE_BROKER_ACK_TIME_BUFFER || "500", 10);
const EVENT_STORE_BROKER_ACK_AMOUNT_BUFFER = parseInt(process.env.EVENT_STORE_BROKER_ACK_AMOUNT_BUFFER || "1000", 10);
const EVENT_STORE_BROKER_ACK_TIMEOUT = parseInt(process.env.EVENT_STORE_BROKER_ACK_TIMEOUT || "120000", 10);
const EVENT_STORE_BROKER_PULL_MESSAGE_INTERVAL_MILLIS = parseInt(process.env.EVENT_STORE_BROKER_PULL_MESSAGE_INTERVAL_MILLIS || "150", 10);
const EVENT_STORE_BROKER_PULL_MESSAGE_AMOUNT = parseInt(process.env.EVENT_STORE_BROKER_PULL_MESSAGE_AMOUNT || "1000", 10);
const EVENT_STORE_BROKER_PULL_MESSAGE_TIMEOUT = parseInt(process.env.EVENT_STORE_BROKER_PULL_MESSAGE_TIMEOUT || "120000", 10);
const EVENT_STORE_BROKER_SUBSCRIPTION_ACK_DEADLINE = parseInt(process.env.EVENT_STORE_BROKER_SUBSCRIPTION_ACK_DEADLINE || "60", 10);
const EVENT_STORE_BROKER_STATS_PERIOD = parseInt(process.env.EVENT_STORE_BROKER_STATS_PERIOD || "120000", 10);
const EVENT_STORE_BROKER_STATS_OLD_MSG_THRESHOLD = parseInt(process.env.EVENT_STORE_BROKER_STATS_OLD_MSG_THRESHOLD || "120000", 10);
const EVENT_STORE_BROKER_STATS_OLD_MSG_STATE_FILTER = process.env.EVENT_STORE_BROKER_STATS_OLD_MSG_STATE_FILTER || Object.values(MSG_STATES).join(',');
const EVENT_STORE_BROKER_STATS_PULL_ACK_LEN = parseInt(process.env.EVENT_STORE_BROKER_STATS_PULL_ACK_LEN || "0", 10);
const EVENT_STORE_BROKER_STATS_SLOW_ACK = parseInt(process.env.EVENT_STORE_BROKER_STATS_SLOW_ACK || "5000", 10);
const EVENT_STORE_BROKER_STATS_SLOW_PULL = parseInt(process.env.EVENT_STORE_BROKER_STATS_SLOW_PULL || "5000", 10);



class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription, disableListener = false, preParseFilter = null, projectId }) {
        /**
         * GCP project ID
         */
        this.projectId = projectId || googleCredentials?.project_id;
        /**
         * EventoSourcing pubsub-Topic Name
         */
        this.eventsTopic = eventsTopic;
        /**
         * EventSourcing pubsub-Subscription name
         */
        this.eventsTopicSubscription = eventsTopicSubscription;
        /**
         * if true, this PubSubBroker will not listen for messages
         */
        this.disableListener = disableListener;
        /**
         * Filter to ignore a message before it has to be parsed (JSON.parse)
         */
        this.preParseFilter = preParseFilter;
        /**
         * Map of Aggregate types and Event types this backend is interested at
         */
        this.aggregateEventsMap = null;
        /**
         * flags to indicate if the message listeneer is stopped
         */
        this.isStopped = false;
        /**
         * flag to indicate if we are currently pulling messages and shoul wait
         */
        this.waitingForPullingMessageCompletion = false;
        /**
         * current pulling messages start time
         */
        this.pullingMessageStartTs = 0;
        /**
         * Total retained messages Metadata.
         * Map of messageId vs {id: messageId, srvTs: serverTimestamp, ackId: ackId, ackCnt: ackIds count, state: state,  stateTs: stateTimestamp }
         */
        this.retainedMessagesMetadata = {};
        /**
         * Total messages pulled without ack COUNT
         */
        this.retainedMessagesCount = 0;
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Subject();
        /**
         * Rx Subject for acknowledge messages
         */
        this.messagesToAcknowledge$ = new Subject();
        /**
         * Messages pull stats
         */
        this.pullStats = [];
        /**
         * Messages ack stats
         */
        this.ackStats = [];
        /**
         * Rx Subject for stopping listening messages
         */
        this.messageListenerDestroyer$ = new Subject();
        /**
         * Rx BehaviorSubject for starting listening messages
         */
        this.aggregateEventsMapConfiguredSubject$ = new BehaviorSubject();
        /**
         * unique sender id, all messages will be mark with this sender id
         */
        this.senderId = uuidv4();
        /**
         * this flags turns on (set on true) when the max messages quota is exceed so we should wait and stop listening new messages for a while
         */
        this.shouldShowStopPullingMessage = false;
        /**
         * EventSourcing pubsub-Client
         */
        this.pubsubClient = new PubSub({});
        /**
         * EventSourcing pubSub-subscription client
         */
        this.restartSubscriptionClient();
        /**
         * EventSourcing pubsub-Topic
         */
        this.topic = this.pubsubClient.topic(eventsTopic, {
            batching: {
                maxMessages: EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MSG,
                maxMilliseconds: EVENT_STORE_BROKER_MAX_PUBLISHER_BATCH_MAX_MILLIS,
            }
        }
        );
    }

    restartSubscriptionClient() {
        if (this.subscriptionClient != null) this.subscriptionClient.close()
            .then(() => ConsoleLogger.w(`EventStore.PubSubBroker.restartSubscriptionClient: Current coneection has been closed`));
        this.subscriptionClient = new v1.SubscriberClient({});
        if (!this.disableListener) {
            this.formattedSubscription = this.subscriptionClient.subscriptionPath(this.projectId, this.eventsTopicSubscription);
            this.subscriptionTopic = `projects/${this.projectId}/topics/${this.eventsTopic}`;
            /**
             * options to pass when invoking pubsub.pull request
             */
            this.pubsubRequestOption = {
                subscription: this.formattedSubscription,
                maxMessages: EVENT_STORE_BROKER_PULL_MESSAGE_AMOUNT,
                allowExcessMessages: false,
            }
            /**
             * Starts message ackowledger flow
             */
            this.startMessageAcknowledged();
        }
    }


    /**
     * Starts Broker connections
     * Returns an Obserable that resolves to each connection result
     */
    start$() {
        return new Observable(observer => {
            if (this.disableListener) {
                observer.next(`Event Store onStart PubSub Broker listener DISABLED`);
                observer.complete();
                return;
            }

            this.startMessageListener();

            //start stats visualizer
            if (EVENT_STORE_BROKER_STATS_PERIOD > 0)
                this.intervalID = setInterval(
                    (function (self) {
                        return function () {
                            if (EVENT_STORE_BROKER_STATS_PULL_ACK_LEN > 0) {
                                ConsoleLogger.i(`EventStore.PubSubBroker.stats: pull stats: ${JSON.stringify(self.pullStats)}`);
                                ConsoleLogger.i(`EventStore.PubSubBroker.stats: ack stats: ${JSON.stringify(self.ackStats)}`);
                            }

                            const msgStatsToShow = Object.values(self.retainedMessagesMetadata)
                                .filter(msg => ((Date.now() - msg.srvTs) > EVENT_STORE_BROKER_STATS_OLD_MSG_THRESHOLD) && EVENT_STORE_BROKER_STATS_OLD_MSG_STATE_FILTER.includes(msg.state))
                                .map(msg => {
                                    msg.srvTsAge = Date.now() - msg.srvTs;
                                    msg.stateAge = Date.now() - msg.stateTs;
                                    return msg;
                                });
                            if (msgStatsToShow.length > 0) {
                                ConsoleLogger.w(`EventStore.PubSubBroker.stats: Retained messages (${msgStatsToShow.length}): ${JSON.stringify(msgStatsToShow)}`);
                                //ConsoleLogger.w(`EventStore.PubSubBroker.stats: Retained messages (${msgStatsToShow.length})`);
                            }
                        };
                    })(this),
                    (EVENT_STORE_BROKER_STATS_PERIOD)
                );

            observer.next(`Event Store PubSub Broker listening: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            observer.complete();
        });
    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected
     */
    stop$() {
        return new Observable(observer => {

            if (this.messageListenerSubscription) this.messageListenerSubscription.unsubscribe();
            if (this.messagesToAcknowledgeSubscription) this.messagesToAcknowledgeSubscription.unsubscribe();

            observer.next(`Event Store onStop PubSub Broker listener DISABLED`);
            observer.complete();
        });

    }

    /**
     * Publish data (or an array of data) throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {*} event Object or Array of objects to send
     */
    publish$(event) {
        return (Array.isArray(event) ? from(event) : of(event)).pipe(
            mergeMap(d => defer(() => this.topic.publish(
                Buffer.from(JSON.stringify(d)),
                {
                    senderId: this.senderId || '',
                    id: d.id || '',
                    et: d.et || '',
                    at: d.at || '',
                    aid: String(d.aid || ''),
                    etv: String(d.etv || ''),
                    ephemeral: String(d.ephemeral || false)
                })
            )),
            last(),
            mapTo(event)
        );
    }

    /**
     * Config aggregate event map
     * @param {*} aggregateEventsMap 
     */
    configAggregateEventMap(aggregateEventsMap) {
        this.aggregateEventsMap = aggregateEventsMap;
        this.aggregateEventsMapConfiguredSubject$.next(aggregateEventsMap);
    }


    /**
     * Returns an Observable that will emit any event related to the given aggregateType (or Array of aggregate types)
     * @param {string} aggregateType aggregateType (or Array of aggregateType) to filter
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        const isAggregateTypeAnArray = Array.isArray(aggregateType);
        const allowAll = isAggregateTypeAnArray ? aggregateType.includes('$ALL') : aggregateType === '$ALL';
        return this.incomingEvents$.pipe(
            filter(msg => msg),
            filter(msg => {
                if (ignoreSelfEvents && msg.attributes.senderId === this.senderId) {
                    msg.acknowledgeMsg({ state: MSG_STATES.IGNORED_SELF });
                    return false;
                }
                return true;
            }),
            filter(msg => {
                if (allowAll || (isAggregateTypeAnArray ? aggregateType.includes(msg.data.at) : msg.data.at === aggregateType)) {
                    this.retainedMessagesMetadata[msg.messageId].state = MSG_STATES.PROCESS_SENT;
                    this.retainedMessagesMetadata[msg.messageId].stateTs = Date.now();
                    return true;
                } else {
                    msg.acknowledgeMsg({ state: MSG_STATES.IGNORED_SENT });
                    return false;
                }
            }),
            map(msg => ({
                ...msg.data, acknowledgeMsg: msg.acknowledgeMsg
            })),
        );

    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    getSubscription$() {
        return defer(() => this.subscriptionClient.getSubscription({ subscription: [this.formattedSubscription] })).pipe(
            catchError((err) => {
                const request = {
                    name: this.formattedSubscription,
                    topic: this.subscriptionTopic,
                    ackDeadlineSeconds: EVENT_STORE_BROKER_SUBSCRIPTION_ACK_DEADLINE
                };
                return this.subscriptionClient.createSubscription(request);
            })
        );
    }

    /**
     * Starts to listen messages
     */
    startMessageListener() {
        this.messageListenerSubscription =
            this.aggregateEventsMapConfiguredSubject$.pipe(
                tap(() => ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: Waiting for aggregateEventsMap to be configured before listening to messages`)),
                filter(u => u),
                switchMap(() => this.getSubscription$()),
                mergeMap(() => interval(EVENT_STORE_BROKER_PULL_MESSAGE_INTERVAL_MILLIS)),
                takeUntil(this.messageListenerDestroyer$),
                filter(() => {
                    if (this.retainedMessagesCount > EVENT_STORE_BROKER_MAX_MESSAGES && !this.shouldShowStopPullingMessage) {
                        this.shouldShowStopPullingMessage = true;
                        ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: Stop pulling messages because of messages without ack limit exceeded: ${this.retainedMessagesCount}, of ${EVENT_STORE_BROKER_MAX_MESSAGES}`);
                    }
                    if (this.retainedMessagesCount < EVENT_STORE_BROKER_MAX_MESSAGES && this.shouldShowStopPullingMessage) {
                        this.shouldShowStopPullingMessage = false;
                        ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: restart pulling messages because of messages without ack limit flatted: ${this.retainedMessagesCount}, of ${EVENT_STORE_BROKER_MAX_MESSAGES}`);
                    }
                    if (this.waitingForPullingMessageCompletion) {
                        return false;
                    }
                    this.waitingForPullingMessageCompletion = true;
                    return this.retainedMessagesCount < EVENT_STORE_BROKER_MAX_MESSAGES;
                }),

                mergeMap(() => {
                    this.pullingMessageStartTs = Date.now();
                    return defer(() => this.subscriptionClient.pull(this.pubsubRequestOption)).pipe(
                        timeout(EVENT_STORE_BROKER_PULL_MESSAGE_TIMEOUT),
                        catchError((err) => {
                            ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: error while pulling messages form pubsub.  elapsed time when error occurred was ${Date.now() - this.pullingMessageStartTs}ms`, err);
                            this.waitingForPullingMessageCompletion = false;
                            if (err.message.includes('DEADLINE_EXCEEDED') || err.message.includes('closed')) {
                                //sometime pubsub respond with the error DEADLINE_EXCEEDED, when this happens we have to restart the client
                                // in order to do so we send a signal to ditch the current listener an create a new one
                                ConsoleLogger.e(`EventStore.PubSubBroker.startMessageListener: error while pulling messages from pubsub. will restart PubSub client`, err);
                                this.restartSubscriptionClient();
                                this.aggregateEventsMapConfiguredSubject$.next(this.aggregateEventsMap);
                            }
                            return of(null);
                        })
                    )
                }),
                filter(u => u),
                mergeMap(([response]) => {
                    this.waitingForPullingMessageCompletion = false;
                    const messages = response.receivedMessages;
                    const pullingTime = Date.now() - this.pullingMessageStartTs;
                    if ((pullingTime) > EVENT_STORE_BROKER_STATS_SLOW_PULL) {
                        ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: subscriptionClient.pull(${messages.length}) is too slow: ${pullingTime}ms`);
                    }
                    if (EVENT_STORE_BROKER_STATS_PULL_ACK_LEN > 0) {
                        let dataLen = 0;
                        for (let receivedMessage of response.receivedMessages) {
                            dataLen += receivedMessage.message.data.length;
                        }
                        this.pullStats.push({
                            initTs: this.pullingMessageStartTs,
                            endTs: Date.now(),
                            diffTs: pullingTime,
                            len: response.receivedMessages.length,
                            dataLen,
                            messages: messages.length
                        });
                        this.pullStats = this.pullStats.slice(-1 * EVENT_STORE_BROKER_STATS_PULL_ACK_LEN)
                    }

                    return from(messages);
                }),
            ).subscribe(
                (rawData) => {
                    const message = rawData.message;
                    message.ackId = rawData.ackId;
                    if (this.retainedMessagesMetadata[rawData.message.messageId]) {
                        const msgMetadata = this.retainedMessagesMetadata[rawData.message.messageId];
                        msgMetadata.ackCnt++;
                        msgMetadata.ackId = rawData.ackId;
                        ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: event with id=${rawData.message.messageId} resent by pubsub. ackId had been updated.`);
                        if (msgMetadata.state === MSG_STATES.ACK_ERROR) {
                            ConsoleLogger.w(`EventStore.PubSubBroker.startMessageListener: event with id=${rawData.message.messageId} was in ACK_ERROR state. will proceed to ack it immediately`);
                            this.messagesToAcknowledge$.next({ ackId: msgMetadata.ackId, id: msgMetadata.id });
                        }
                        return;
                    }

                    this.retainedMessagesMetadata[rawData.message.messageId] = {
                        id: rawData.message.messageId,
                        dataLen: rawData.message.data.length,
                        srvTs: Date.now(),
                        ackId: rawData.ackId,
                        ackCnt: 1,
                        state: MSG_STATES.RECEIVED,
                        stateTs: Date.now(),
                    };
                    const messageMetadata = this.retainedMessagesMetadata[rawData.message.messageId];
                    this.retainedMessagesCount++;

                    const hasEventSourcingAttributes = message.attributes.at != null && message.attributes.at != '' && message.attributes.et != null && message.attributes.et != ' ' && message.attributes.aid != null && message.attributes.aid != '';
                    let eventTypeConfig;
                    //if the message already has the headers/attributes we can skip the message before parsing
                    if (hasEventSourcingAttributes) {
                        messageMetadata.type = `${message.attributes.at}:${message.attributes.et}`;
                        const atMap = this.aggregateEventsMap[message.attributes.at] || this.aggregateEventsMap.$ALL;
                        eventTypeConfig = !atMap ? undefined : (atMap[message.attributes.et] || atMap.$ALL);
                        if (!eventTypeConfig) {
                            messageMetadata.state = MSG_STATES.IGNORED_BPAR;
                            messageMetadata.stateTs = Date.now();
                            this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                            return;
                        }
                    }

                    //if the developer assigned preParse filters using the attributes (Eg. ReplciaSet instance filter) the we can discard messages before parsing
                    if (hasEventSourcingAttributes && this.preParseFilter != null && !this.preParseFilter(message.attributes)) {
                        messageMetadata.state = MSG_STATES.IGNORED_PPF;
                        messageMetadata.stateTs = Date.now();
                        this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                        return;
                    }

                    const msgEvt = {
                        data: JSON.parse(message.data),
                        id: message.id,
                        attributes: message.attributes,
                        correlationId: message.attributes.correlationId,
                        ackId: message.ackId,
                        messageId: message.messageId
                    };

                    //ConsoleLogger.d(`EventStore.PubSubBroker.startMessageListener: Received message, Id: ${msgEvt.id}, aT: ${msgEvt.data.at}, eT: ${msgEvt.data.et}`)

                    if (!eventTypeConfig) {
                        const atMap = this.aggregateEventsMap[msgEvt.data.at] || this.aggregateEventsMap.$ALL;
                        eventTypeConfig = !atMap ? undefined : atMap[msgEvt.data.et] || atMap.$ALL;
                    }
                    if (!eventTypeConfig) {
                        // If there are not handler for this message, it means that this microservice is not interested on this information
                        //ConsoleLogger.d(`ACK Before: Message does not matter for this backend, Id: ${msgEvt.id}, at: ${msgEvt.data.at}, et: ${msgEvt.data.et}`)
                        messageMetadata.state = MSG_STATES.IGNORED_APAR;
                        messageMetadata.stateTs = Date.now();
                        this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                        return;
                    }

                    const autoAck = eventTypeConfig.autoAck;
                    const processOnlyOnSync = eventTypeConfig.processOnlyOnSync;

                    if ((autoAck != null && autoAck) || processOnlyOnSync || (autoAck == null && EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED != null && !EVENT_STORE_BROKER_MESSAGE_ACK_AFTER_PROCESSED)) {
                        messageMetadata.state = MSG_STATES.AUTO_ACK;
                        messageMetadata.stateTs = Date.now();
                        this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                    } else {
                        messageMetadata.state = MSG_STATES.PROCESS_READY;
                        messageMetadata.stateTs = Date.now();
                        this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                        const self = this;
                        msgEvt.acknowledgeMsg = (ops = {}) => {
                            if (messageMetadata.state < MSG_STATES.IGNORED) { // avoids doing ack duplicate
                                messageMetadata.state = ops.state || MSG_STATES.PROCESS_DONE;
                                messageMetadata.stateTs = Date.now();
                                this.messagesToAcknowledge$.next({ ackId: messageMetadata.ackId, id: messageMetadata.id });
                            }
                        };
                    }
                    this.incomingEvents$.next(msgEvt);
                },
                (err) => {
                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageListener: Observable terminate due to error`, err);
                    process.exit(1);
                },
                () => {
                    this.isStopped = true;
                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageListener: Observable completed`);
                }
            );
    }

    /**
     * Starts to buffer and acknowledge messages
     */
    startMessageAcknowledged() {
        if (this.messagesToAcknowledgeSubscriptionStarted) return;
        this.messagesToAcknowledgeSubscription = this.messagesToAcknowledge$.pipe(
            filter(u => u),
            bufferTime(EVENT_STORE_BROKER_ACK_TIME_BUFFER, null, EVENT_STORE_BROKER_ACK_AMOUNT_BUFFER),
            filter(buffer => buffer.length > 0),
            mergeMap((buffer) => {
                const messagesToAckIds = [];
                const faultyIckIdMap = {};
                for (let msg of buffer) {
                    if (!faultyIckIdMap[msg.ackId]) {
                        messagesToAckIds.push(msg.ackId);
                    }
                }
                let retryAttempt = 0;
                const ackMessageStartTs = Date.now();
                return of(messagesToAckIds).pipe(
                    mergeMap((ackIds) => {
                        const ackRequest = {
                            subscription: this.formattedSubscription,
                            ackIds: ackIds.filter(ackId => faultyIckIdMap[ackId] == null),
                        };
                        return defer(() => this.subscriptionClient.acknowledge(ackRequest)).pipe(
                            timeout(EVENT_STORE_BROKER_ACK_TIMEOUT),
                        )
                    }),
                    tap((result) => {
                        const ackTimeDiff = Date.now() - ackMessageStartTs;
                        if ((ackTimeDiff) > EVENT_STORE_BROKER_STATS_SLOW_ACK) {
                            ConsoleLogger.w(`EventStore.PubSubBroker.startMessageAcknowledged: subscriptionClient.acknowledge(${messagesToAckIds.length}) is too slow: ${ackTimeDiff}ms`);
                        }
                        if (EVENT_STORE_BROKER_STATS_PULL_ACK_LEN > 0) {
                            this.ackStats.push({
                                initTs: ackMessageStartTs,
                                endTs: Date.now(),
                                diffTs: ackTimeDiff,
                                len: messagesToAckIds.length,
                            });
                            this.ackStats = this.ackStats.slice(-1 * EVENT_STORE_BROKER_STATS_PULL_ACK_LEN)
                        }
                        this.retainedMessagesCount -= messagesToAckIds.length;
                        for (let msg of buffer) {
                            if (!faultyIckIdMap[msg.ackId]) {
                                delete this.retainedMessagesMetadata[msg.id];
                            }
                        }
                    }),

                    retryWhen((errors) =>
                        errors.pipe(
                            tap((err) => {

                                if (err.message.includes('DEADLINE_EXCEEDED')) {
                                    //sometime pubsub respond with the error DEADLINE_EXCEEDED, when this happens we have to restart the client
                                    // in order to do so we send a signal to ditch the current listener an create a new one
                                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageAcknowledged: error while send acknowledges messages.  will restart PubSub client`, err);
                                    this.restartSubscriptionClient();
                                    this.aggregateEventsMapConfiguredSubject$.next(this.aggregateEventsMap);
                                }

                                const match = err.message.match(/ack_id=([^)]+)/);
                                const faultyAckId = (match && match[1]) ? match[1] : null;
                                ConsoleLogger.w(`EventStore.PubSubBroker.startMessageAcknowledged: error while send acknowledges messages from pubsub. message=${err.message}, faultyAckId=${faultyAckId}, will remove faulty ackId and proceed`, err);
                                if (faultyAckId != null) {
                                    faultyIckIdMap[faultyAckId] = true;
                                    for (let msg of buffer) {
                                        if (msg.ackId === faultyAckId) {
                                            if (this.retainedMessagesMetadata[msg.id]) {
                                                this.retainedMessagesMetadata[msg.id].state = MSG_STATES.ACK_ERROR;
                                                this.retainedMessagesMetadata[msg.id].stateTs = Date.now();
                                                break;
                                            }
                                        }
                                    }
                                }
                                retryAttempt += 1;
                                if (retryAttempt === EVENT_STORE_BROKER_MAX_RETRY_ATTEMPTS) {
                                    ConsoleLogger.e(`EventStore.PubSubBroker.startMessageAcknowledged: STOPPED`);
                                    this.messageListenerDestroyer$.next("stop");
                                }
                            }),
                            tap((err) => ConsoleLogger.e(`EventStore.PubSubBroker.startMessageAcknowledged: error trying to invoke subscriptionClient.acknowledge with the following ackIds: ${JSON.stringify(messagesToAckIds)}`, err)),
                            delayWhen(() => timer(2000))
                        )
                    ),

                );
            })
        ).subscribe(
            () => {
                if (this.isStopped) {
                    process.exit(1);
                }
            },
            (err) => {
                ConsoleLogger.e('EventStore.PubSubBroker.startMessageAcknowledged: Failed to acknowledged messages', err);
                process.exit(1);
            },
            () => {
                ConsoleLogger.e('EventStore.PubSubBroker.startMessageAcknowledged: messagesToAcknowledge has completed!');
            }
        );
        this.messagesToAcknowledgeSubscriptionStarted = true;
        ConsoleLogger.i(`EventStore.PubSubBroker.startMessageListener: aggregateEventsMap configured, will start listening messages`);
    }

}

module.exports = PubSubBroker;