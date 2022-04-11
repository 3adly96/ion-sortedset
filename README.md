### ION-SORTEDSET
ION-SORTEDSET is a time series that produces and consumes redis sorted sets in non-blocking manner. ION-SORTEDSET listens to sorted sets that carries an event that will be excecuted in a certain future timestamp in a subprocess.



Full Example
```jsx

const SortedSetManager = require('ion-sortedset');
const sm = new SortedSetManager({ url: "redis://127.0.0.1:6379" });

let idsList = [];

const consumer = sm.consumer({
    timestamp: Date.now(), // the timestamp that the listener will start from
    key: '3enab',
    keepAlive: true,
    segmantDuration: 1000, // the timestamps wil be retrieved as blocks of 1 second range of timestamps
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

setTimeout(() => {
    consumer.close();
}, 100000)

```