// mock.js
const mqtt = require('mqtt')
const Mock = require('mockjs')
const EMQX_SERVER = 'mqtt://localhost:1883'
const CLIENT_NUM = 10
const STEP = 5000 // Data interval in ms
const AWAIT = 5000 // Sleep time after data be written once to avoid data writing too fast
const CLIENT_POOL = []
startMock()
function sleep(timer = 100) {
  return new Promise(resolve => {
    setTimeout(resolve, timer)
  })
}
async function startMock() {
  const now = Date.now()
  for (let i = 0; i < CLIENT_NUM; i++) {
    const client = await createClient(`mock_client_${i}`)
    CLIENT_POOL.push(client)
  }
  // last 24h every 5s
  const last = 24 * 3600 * 1000
  for (let ts = now - last; ts <= now; ts += STEP) {
    for (const client of CLIENT_POOL) {
      const mockData = generateMockData()
      const data = {
        ...mockData,
        id: client.clientId,
        area: 0,
        ts,
      }
      client.publish('sensor/data', JSON.stringify(data))
    }
    const dateStr = new Date(ts).toLocaleTimeString()
    console.log(`${dateStr} send success.`)
    await sleep(AWAIT)
  }
  console.log(`Done, use ${(Date.now() - now) / 1000}s`)
}
/**
 * Init a virtual mqtt client
 * @param {string} clientId ClientID
 */
function createClient(clientId) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(EMQX_SERVER, {
      clientId,
    })
    client.on('connect', () => {
      console.log(`client ${clientId} connected`)
      resolve(client)
    })
    client.on('reconnect', () => {
      console.log('reconnect')
    })
    client.on('error', (e) => {
      console.error(e)
      reject(e)
    })
  })
}
/**
* Generate mock data
*/
function generateMockData() {
 return {
   "temperature": parseFloat(Mock.Random.float(22, 100).toFixed(2)),
   "humidity": parseFloat(Mock.Random.float(12, 86).toFixed(2)),
   "volume": parseFloat(Mock.Random.float(20, 200).toFixed(2)),
   "PM10": parseFloat(Mock.Random.float(0, 300).toFixed(2)),
   "pm25": parseFloat(Mock.Random.float(0, 300).toFixed(2)),
   "SO2": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "NO2": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "CO": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "area": Mock.Random.integer(0, 20),
   "ts": 1596157444170,
 }
}