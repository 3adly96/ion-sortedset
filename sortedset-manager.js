
const NanoTimer = require('nanotimer');
const debug = require('debug')('ion-sortedset');

module.exports = class RedisTimeMachine {

  constructor({ url }) {
    this.redisClient = require('./connect').createClient({ url });
    this.timer = new NanoTimer();
    this.ftimestamp = 0;
  }

  async listenToSortedSet({ key, timestamp, onData }) {
    let now = Date.now();
    if (Math.floor(timestamp) + 1 > now) {
      await this.delay('1m');
      return this.listenToSortedSet({ key: key, timestamp: timestamp, onData })
    }
    await this.delay('190m');
    let args = [key, timestamp, Math.floor(timestamp) + 200, 'WITHSCORES'];
    let result = await this.redisClient.zrangebyscore(...args);
    if (result.length !== 0) {
      if (timestamp <= Date.now()) {
        for (let i = 0; i < result.length; i += 2) {
          let resultObj = JSON.parse(result[i]);
          let p = {
            id: result[i + 1],
            value: resultObj,
          }
          onData(p);
        }
      }
      else {
        await this.delay('500m');
        return this.listenToSortedSet({ key: key, timestamp: timestamp, onData });
      }
    }
    this.listenToSortedSet({ key: key, timestamp: Math.floor(timestamp) + 201, onData });
  }

  async emitToSortedSet({ key, json, timestamp }) {
    let args = [key, timestamp];
    debug(`${json.call} will be executed on ${timestamp}`)
    let jsonString = JSON.stringify(json);
    args.push(jsonString);
    await this.redisClient.zadd(...args);
  }

  async delay(time) {
    return new Promise((resolve, reject) => {
      this.timer.setTimeout(() => {
        resolve(true);
      }, '', time)
    })
  }
}