const taos = require('@tdengine/websocket');

const url = process.env.TDENGINE_CLOUD_URL;
const topic = 'topic_meters';
const topics = [topic];
const groupId = 'group1';
const clientId = 'client1';

async function prepare() {
  if (!url) {
    console.error('Please set TDENGINE_CLOUD_URL');
    process.exit(1);
  }
  let sql =
    'create topic if not exists ' + topic + ' as select * from test.meters';
  let conf = new taos.WSConfig(url);
  try {
    let conn = await taos.sqlConnect(conf);
    await conn.exec(sql);
  } catch (err) {
    console.error(
      `Failed to create topic, topic: ${topic}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
    );
    throw err;
  }
}

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
  await prepare();
  let consumer = await createConsumer();

  try {
    // subscribe
    await consumer.subscribe(topics);
    console.log(`Subscribe topics successfully, topics: ${topics}`);

    for (let i = 0; i < 100; i++) {
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
      `Failed to consumer, ErrCode: ${err.code}, ErrMessage: ${err.message}`
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
