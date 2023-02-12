
const emptyCb = () => { };
const path = require('path');

class Consumer {
    constructor({ url, key, executionkey, timestamp, keepAlive, onMessage, onError, onClose, segmantDuration }) {

        this.url             = url;
        this.key             = key;
        this.executionkey    = executionkey
        this.timestamp       = timestamp;
        this.segmantDuration = segmantDuration
        this.keepAlive       = keepAlive || false;
        this.onMessage       = onMessage || emptyCb;
        this.onError         = onError || emptyCb;
        this.onClose         = onClose || emptyCb;
        this.child           = null;
        this.forcedToClose   = false;

        this.fork();
        this.processListeners();
    }

    _faultHandler() {
        if (this.keepAlive && !this.forcedToClose) {
            this.child.kill();
            setTimeout(() => { this.fork() }, 1000);
        }
    }

    close() {
        this.forcedToClose = true;
        this.child.kill();
    }

    exitHandler(child, exitCode) {
        child.kill();
        process.exit();
    }

    processListeners(){
        process.on('exit', this.close.bind(this))
            .on('uncaughtException', this.close.bind(this))
            .on('SIGINT', this.close.bind(this))
            .on('SIGUSR1', this.close.bind(this))
            .on('SIGUSR2', this.close.bind(this))
    }

    fork() {
        this.child = require('child_process').fork(path.resolve(__dirname, './consumer-process.js'), {
            env: {
                ION_SS_URL: this.url,
                ION_SS_KEY: this.key,
                ION_SS_EXECUTEKEY: this.executionkey,
                ION_SS_TIMESTAMP: this.timestamp,
                ION_SS_SEGMANT: this.segmantDuration
            }
        });

        this.child.on('message', this.onMessage);
        this.child.on('error', () => {
            this._faultHandler();
            this.onError();
        });
        this.child.on('close', () => {
            this._faultHandler();
            this.onClose();
        });
        this.child.on('exit', ()=> {
            this._faultHandler();
            this.onClose();
        })
    }
}

class Producer {
    constructor({ url }) {
        const RedisTimeMachine = require('./sortedset-manager');
        this.sortedSetCli = new RedisTimeMachine({ url });
    }

    emit({ json, key, timestamp }) {
        return this.sortedSetCli.emitToSortedSet({ json, key, timestamp });
    }

    setAsExecuted({ executionkey, id, json }) {
        return this.sortedSetCli.setAsExecuted({ executionkey, id, json });
    }
}


class SortedSetManager {
    constructor({ url }) {
        if (!url) throw Error(`url is missing`);
        this.url = url;
    }

    consumer(args) {
        return (new Consumer({ url: this.url, ...args }));
    }

    producer() {
        return (new Producer({ url: this.url }));
    }
}

module.exports = SortedSetManager
