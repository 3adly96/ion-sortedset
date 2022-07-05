
const NanoTimer = require('nanotimer');
const debug = require('debug')('ion-sortedset');

module.exports = class RedisTimeMachine {

  constructor({ url }) {
    this.redisClient = require('./connect').createClient({ url });
    this.timer = new NanoTimer();
    this.ftimestamp = 0;
  }

  async listenToSortedSet({ key, timestamp, onData, segmantDuration }) {
    try {
      let now = Date.now();
      if (Math.floor(timestamp) + 1 > now) {
        await this.delay('1m');
        return this.listenToSortedSet({ key: key, timestamp: timestamp, onData, segmantDuration })
      }
      await this.delay(segmantDuration + 'm');
      let args = [key, timestamp, Math.floor(timestamp) + Math.floor(segmantDuration)  , 'WITHSCORES'];
      let result = await this.redisClient.zrangebyscore(...args);
      if (result.length !== 0) {
        let remArgs = [key, '-INF', result[result.length-1]];
        if (timestamp <= Date.now()) {
          for (let i = 0; i < result.length; i += 2) {
            let resultObj = JSON.parse(result[i]);
            let p = {
              id: result[i + 1],
              value: resultObj,
            }
            onData(p);
          }
          this.redisClient.zremrangebyscore(...remArgs)
        }
        else {
          await this.delay('500m');
          return this.listenToSortedSet({ key: key, timestamp: timestamp, onData, segmantDuration });
        }
      }
      this.listenToSortedSet({ key: key, timestamp: Math.floor(timestamp) + Math.floor(segmantDuration) + 1, onData, segmantDuration });
    } catch (err) {
      debug('===> Error at listenToSortedSet <===');
      debug(err);
    }
  }

  async emitToSortedSet({ key, json, timestamp }) {
    try {
      const isExist = await this.isCallExist(json.call);
      if(isExist) return {error:'function already scheduled'};
      let args = [key, timestamp];
      debug(`${json.call} will be executed on ${timestamp}`);
      let jsonString = JSON.stringify(json);
      args.push(jsonString);
      this.redisClient.zadd(...args);
      this.redisClient.set(`schedFunc:${json.call}`, true, 'PXAT', parseInt(timestamp));
      return 'scheduled';
    } catch (err) {
      debug('===> Error at emitToSortedSet <===');
      debug(err);
    }
  }

  async isCallExist(func) {
    return await this.redisClient.get(`schedFunc:${func}`);
  }

  async delay(time) {
    return new Promise((resolve, reject) => {
      this.timer.setTimeout(() => {
        resolve(true);
      }, '', time)
    })
  }
}