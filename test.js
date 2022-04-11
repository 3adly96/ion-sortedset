

const SortedSetManager = require('./index.js');
const sm = new SortedSetManager({ url: "redis://127.0.0.1:6379" });

let idsList = [];

const consumer = sm.consumer({
    timestamp: Date.now(),
    key: '3enab',
    keepAlive: true,
    segmantDuration: 1000, // the timestamps wil be retrieved with blocks of 1 second 
    onMessage: (d) => {
        idsList.push(d.id);
        console.log(`got message`, d);
    },
    onError: (d) => { console.log(`got error`, d) },
    onClose: () => { console.log(`got close`) },
})

const producer = sm.producer();
generatetimestamp();

async function generatetimestamp() {
    var ssjson;
    var ssKey = '3enab';
    ftimestamp = (Date.now() + 40000) + '';
    for (var i = 0; i < 1000; i++) {
        for (var j = 0; j < 5; j++) {
            ssjson = {
                method: 'timestamper.time',
                data: {
                    a: ftimestamp + '' + j
                }
            }
            await producer.emit({ key: ssKey, json: ssjson, timestamp: ftimestamp });
        }
        ftimestamp++;
    }
}

// setTimeout(() => {
//     consumer.close();
// }, 100000)