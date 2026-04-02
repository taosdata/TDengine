package com.taosdata.jdbc.test;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSDBMultiThreadTest {

	public static void main(String[] args) {

		String osName = System.getProperties().getProperty("os.name");
		String directory = null;

		if (osName.indexOf("Window") >= 0 || osName.indexOf("Mac") >= 0) {
			directory = System.getProperty("user.dir");
		} else {
			File dir = new File(System.getProperty("user.dir"));
			directory = dir.getParent();
		}

		String host = "192.168.100.128";
		int threadNum = 3;
		System.out.println("arguments format : host threadNum");
		if (args.length >= 1) {
			host = args[0];
		}
		if (args.length >= 2) {
			threadNum = Integer.parseInt(args[1]);
		}

		// ---------------------------------------------------------------
		System.setProperty("log4jhome", directory);

		final Logger logger = LoggerFactory.getLogger(TSDBMultiThreadTest.class);
		final ConcurrentHashMap<String, TSDBMultiThreadRun> producers = new ConcurrentHashMap<String, TSDBMultiThreadRun>();

		for (int i = 1; i <= threadNum; i++) {
			TSDBMultiThreadRun t = new TSDBMultiThreadRun(i, 100, host);
			new Thread(t, "producer_" + i).start();
			producers.put(t.getTableName(), t);
		}

		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleAtFixedRate(new Runnable() {
			public void run() {
				for (Entry<String, TSDBMultiThreadRun> entry : producers.entrySet()) {
					StringBuffer sb = new StringBuffer();
					sb.append("table:").append(entry.getKey()).append(", insert into : ")
							.append(entry.getValue().getInsertCount()).append(" , cost : ")
							.append(entry.getValue().getCost()).append(" .");
					logger.info(sb.toString());
				}
			}
		}, 10, 20, TimeUnit.SECONDS);
	}

}
