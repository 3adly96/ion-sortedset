
const NanoTimer = require('nanotimer');
const debug = require('debug')('ion-sortedset');

module.exports = class RedisTimeMachine {

  constructor({ url }) {
    this.redisClient  = require('./connect').createClient({ url });
    this.timer        = new NanoTimer();
    this.cleanerTimer = new NanoTimer();
    this.cleanUpDelay = 60;
    this.ftimestamp   = 0;
    this.timestamp    = null;
  }

  async listenToSortedSet({ key, timestamp, onData, segmantDuration }) {
    this.timestamp = timestamp;
    try {
      if (Math.floor(timestamp) + 500 > Date.now()) {
        await this.delay(segmantDuration + 'm');
        return this.listenToSortedSet({ key, timestamp, onData, segmantDuration })
      }
      let args = [key, timestamp, Math.floor(timestamp) + Math.floor(segmantDuration)  , 'WITHSCORES'];
      let functions = await this.redisClient.zrangebyscore(...args);
      if (functions.length !== 0) {
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
      this.listenToSortedSet({ key, timestamp: Math.floor(timestamp) + Math.floor(segmantDuration) + 1, onData, segmantDuration });
    } catch (err) {
      debug('===> Error at listenToSortedSet <===');
      debug(err);
    }
  }

  async setCleaner({ executionkey, key }) {
    let retries = [];
    const interv = 1000 * (this.cleanUpDelay / 2); //delays clean up for a minute half here and half in the redis call
    await this.delayClean(`${interv}m`);
    const timestamp = Date.now()
    const args            = [key, '-INF', timestamp - interv];
    const execArgs        = [executionkey, '-INF', timestamp - interv];
    const orders          = await this.redisClient.zrangebyscore(...args);
    const executedOrders  = await this.redisClient.zrangebyscore(...execArgs);
    // console.log('executedOrders',executedOrders)
    // console.log('orders',orders)
    for (const order of orders) {
      if (!executedOrders.includes(order)) retries.push(order);
    }
    // console.log('retries',retries)
    for (const retry of retries) {
      this.redisClient.zadd(key, Date.now() + 5000, retry);
    }
    this.redisClient.zremrangebyscore(...args);
    this.redisClient.zremrangebyscore(...execArgs);
    this.setCleaner({ executionkey, key });
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

  setAsExecuted({ executionkey, id, json }) {
    // this.redisClient.zrem(key, JSON.stringify(json));
    this.redisClient.zadd(executionkey, id, JSON.stringify(json));
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