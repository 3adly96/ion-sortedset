
const NanoTimer = require('nanotimer');
const debug = require('debug')('ion-sortedset');

module.exports = class RedisTimeMachine {

  constructor({ url }) {
    this.redisClient  = require('./connect').createClient({ url });
    this.timer        = new NanoTimer();
    this.cleanerTimer = new NanoTimer();
    this.ftimestamp   = 0;
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
      let functions = await this.redisClient.zrangebyscore(...args);
      if (functions.length !== 0) {
        if (timestamp <= Date.now()) {
          for (let i = 0; i < functions.length; i += 2) {
            let functionId = functions[i + 1];
            let functionObj = JSON.parse(functions[i]);
            let p = {
              id: functionId,
              value: functionObj,
            }
            onData(p);
          }
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

  async setCleaner({ key }) {
    let retries = [];
    const interv = 1000*60;
    await this.delayClean(`${interv}m`);
    const timestamp = Date.now();
    const args = [key, '-INF', timestamp];
    const orders = await this.redisClient.zrangebyscore(...args);
    for(const order of orders) {
      if(!order.includes('"isExecuted":true')) retries.push(order);
    }
    for(const retrie of retries) {
      this.redisClient.zadd(key, Date.now()+5000, retrie);
    }
    this.redisClient.zremrangebyscore(...args);
    this.setCleaner({ key });
  }

  async emitToSortedSet({ key, json, timestamp }) {
    try {
      const isExist = await this.isCallExist(`${json.args.call}:${json.id}`);
      if(isExist) return {error:`function with id ${json.id} already scheduled`};
      let args = [key, timestamp];
      debug(`${json.call} will be executed on ${timestamp}`);
      let jsonString = JSON.stringify(json);
      args.push(jsonString);
      this.redisClient.zadd(...args);
      this.redisClient.set(`schedFunc:${json.args.call}:${json.id}`, true, 'PXAT', parseInt(timestamp)); 
      return 'scheduled';
    } catch (err) {
      debug('===> Error at emitToSortedSet <===');
      debug(err);
    }
  }

  setAsExecuted({ key, id, json }) {
    // this.redisClient.zrem(key, JSON.stringify(json));
    json.isExecuted = true;
    this.redisClient.zadd(key, id, JSON.stringify(json));
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

  async delayClean(time) {
    return new Promise((resolve, reject) => {
      this.cleanerTimer.setTimeout(() => {
        resolve(true);
      }, '', time)
    })
  }
}