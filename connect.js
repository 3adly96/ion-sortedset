
const Redis = require("ioredis");

const runTest = async (redis) => {
  const key = `${new Date().getTime()}`;
  await redis.set(key, "Redis Test Done.");
  let data = await redis.get(key);
  console.log(`Test Data: ${data}`);
  redis.del(key);
}

const createClient = ({ url }) => {

  console.log('redis client is connectig to', { url })

  const redis = new Redis(url);

  //register client events
  redis.on('error', (error) => {
    console.log('error', error);
  });

  redis.on('end', () => {
    console.log('shutting down service due to lost Redis connection');
  });
  
  runTest(redis);

  return redis;
}



exports.createClient = createClient;
