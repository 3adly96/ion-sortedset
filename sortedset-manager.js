
const NanoTimer = require('nanotimer');

module.exports = class RedisTimeMachine {

  constructor({ url }) {
    this.redisClient = require('./connect').createClient({ url });
    this.timer = new NanoTimer();
    this.ftimestamp = 0;
  }

  async listenToSortedSet({ key, timestamp, onData }) {
    let result = await this.redisClient.zrangebyscore(...[key, timestamp, timestamp, 'WITHSCORES']);
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
    this.listenToSortedSet({ key: key, timestamp: ++timestamp, onData });
  }

  async emitToSortedSet({ key, json, timestamp }) {
    let args = [key, 'NX', timestamp];
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