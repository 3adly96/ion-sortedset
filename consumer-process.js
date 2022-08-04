const url           = process.env.ION_SS_URL;
const key           = process.env.ION_SS_KEY;
const executionkey  = process.env.ION_SS_EXECUTEKEY;
const timestamp     = process.env.ION_SS_TIMESTAMP;
const segmant       = process.env.ION_SS_SEGMANT;
const debug         = require('debug')('ion-sortedset');


//check if arugment is passed correctly 
if (!url) throw Error('missing arguments');

//load client for that specific event 
const RedisTimeMachine = require('./sortedset-manager.js');
const sortedSetCli = new RedisTimeMachine({ url: url });

// SIGTERM AND SIGINT will trigger the exit event.
process.once("SIGTERM", function () {
    process.exit(0);
});
process.once("SIGINT", function () {
    process.exit(0);
});


/** should keep running 4ever */
const run = async () => {
    await sortedSetCli.listenToSortedSet({
        key: key,
        timestamp: timestamp,
        onData: (result) => {
            process.send(result);
        },
        segmantDuration: segmant
    });
    sortedSetCli.setCleaner({ key, executionkey });
}

run({});
debug(`listening for event: ${timestamp}`);