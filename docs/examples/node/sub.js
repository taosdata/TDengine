const taos = require('@tdengine/websocket');

const cloud_url = process.env.TDENGINE_CLOUD_URL;
const token = process.env.TDENGINE_CLOUD_TOKEN;
const url = `${cloud_url}?token=${token}`;
const topic = 'topic_meters';
const topics = [topic];
const groupId = 'group1';
const clientId = 'client2';

async function createConsumer() {
  let configMap = new Map([
    [taos.TMQConstants.GROUP_ID, groupId],
    [taos.TMQConstants.CLIENT_ID, clientId],
    [taos.TMQConstants.AUTO_OFFSET_RESET, 'earliest'],
    [taos.TMQConstants.WS_URL, url],
    [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
    [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000'],
  ]);
  try {
    // create consumer
    let consumer = await taos.tmqConnect(configMap);
    console.log(
      `Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`
    );
    return consumer;
  } catch (err) {
    console.error(
      `Failed to create websocket consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
    );
    throw err;
  }
}

async function testConsumer() {
  let consumer = await createConsumer();

  try {
    // subscribe
    await consumer.subscribe(topics);
    console.log(`Subscribe topics successfully, topics: ${topics}`);

    for (let i = 0; i < 5000; i++) {
      // poll
      let res = await consumer.poll(1000);
      for (let [key, value] of res) {
        // Add your data processing logic here
        console.log(`data: ${JSON.stringify(value, replacer)}`);
      }
      // commit
      await consumer.commit();
    }

    // seek
    let assignment = await consumer.assignment();
    await consumer.seekToBeginning(assignment);
    console.log('Assignment seek to beginning successfully');

    // clean
    await consumer.unsubscribe();
  } catch (err) {
    console.error(
      `Failed to create websocket consumer, ErrCode: ${err.code}, ErrMessage: ${err.message}`
    );
    throw err;
  } finally {
    if (consumer) {
      await consumer.close();
    }
    taos.destroy();
  }
}

// Custom replacer function to handle BigInt serialization
function replacer(key, value) {
  if (typeof value === 'bigint') {
    return value.toString(); // Convert BigInt to string
  }
  return value;
}

testConsumer();
