const taos = require("@tdengine/client");

const conn = taos.connect({ host: "localhost", database: "power" });
var cursor = conn.cursor();

function runConsumer() {

    // create topic 
    cursor.execute("create topic topic_name_example as select * from meters");

    let consumer = taos.consumer({
        'group.id': 'tg2',
        'td.connect.user': 'root',
        'td.connect.pass': 'taosdata',
        'msg.with.table.name': 'true',
        'enable.auto.commit': 'true'
    });
    
    // subscribe the topic just created.
    consumer.subscribe("topic_name_example");

    // get subscribe topic list
    let topicList = consumer.subscription();
    console.log(topicList);

    for (let i = 0; i < 5; i++) {
        let msg = consumer.consume(100);
        console.log(msg.topicPartition);
        console.log(msg.block);
        console.log(msg.fields)
        consumer.commit(msg);
        console.log(`=======consumer ${i} done`)
    }

    consumer.unsubscribe();
    consumer.close();

    // drop topic
    cursor.execute("drop topic topic_name_example");
}


try {
    runConsumer();
} finally {

    setTimeout(() => {
        cursor.close();
        conn.close();
    }, 2000);
}
