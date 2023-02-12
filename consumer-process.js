const url           = process.env.ION_SS_URL;
const key           = process.env.ION_SS_KEY;
const executionkey  = process.env.ION_SS_EXECUTEKEY;
const timestamp     = process.env.ION_SS_TIMESTAMP;
const segmant       = process.env.ION_SS_SEGMANT;
const debug         = require('debug')('ion-sortedset');
let sortedSetCli    = {};

//check if arugment is passed correctly 
if (!url) throw Error('missing arguments');

//load client for that specific event 
try{
    const RedisTimeMachine = require('./sortedset-manager.js');
    sortedSetCli = new RedisTimeMachine({ url: url });
} catch (err) {
    process.exit(1);
}

// SIGTERM AND SIGINT will trigger the exit event.
process.on("SIGTERM", function () {
    console.log('consumer process existed SIGTERM')
    process.exit(1);
});

process.on("SIGINT", function () {
    console.log('consumer process existed SIGINT')
    process.exit(1);
});

process.on('error', function (err) {
    console.log(err);
    process.exit(1);
});

process.on('uncaughtException', function (err) {
    // console.log(err);
    process.exit(1);
});


/** should keep running 4ever */
const run = async () => {
    await sortedSetCli.listenToSortedSet({
        key: key,
        timestamp: timestamp,
        onData: (result) => {
            try {
                process.send(result);
            } catch(err){
                console.log(err);
            }
        },
        segmantDuration: segmant
    });
    sortedSetCli.setCleaner({ key, executionkey });
}

run({});
debug(`listening for event: ${timestamp}`);