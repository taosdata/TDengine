import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;


import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.math.*;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;


public class OpentsdbTest{
  //static { System.setProperty("logback.configurationFile", "/home/ubuntu/fang/opentsdb/opentsdbtest/logback.xml");}
  static { System.setProperty("logback.configurationFile", "/etc/opentsdb/logback.xml");}

  public static void main(String args[]) {  

    Logger logger = LogManager.getLogger(OpentsdbTest.class);
    logger.setLevel(Level.OFF);
    // begin to parse argument
    String datadir = "/home/ubuntu/testdata";
    String sqlchoice  = "q1";
    int numOfRows = 1000000;
    int numOfFiles = 0;
    int numOfClients = 1;
    int rowsPerRequest = 1;
    for (int i = 0; i < args.length; ++i) {
        if (args[i].equalsIgnoreCase("-dataDir")) {
            if (i < args.length - 1) {
                datadir = args[++i];
            }
        } else if (args[i].equalsIgnoreCase("-numofFiles")) {
            if (i < args.length - 1) {
                numOfFiles = Integer.parseInt(args[++i]);
            }
        } else if (args[i].equalsIgnoreCase("-rowsPerRequest")) {
            if (i < args.length - 1) {
                rowsPerRequest = Integer.parseInt(args[++i]);
            } 
        } else if (args[i].equalsIgnoreCase("-writeClients")) {
            if (i < args.length - 1) {
                numOfClients = Integer.parseInt(args[++i]);
            }
        } else if (args[i].equalsIgnoreCase("-sql")) {
                sqlchoice = args[++i];
        }
    }
    System.out.println("parameters:\n");


    if (numOfFiles >0) {
      // write data
      System.out.printf("----dataDir:%s\n", datadir);
      System.out.printf("----numOfFiles:%d\n", numOfFiles);
      System.out.printf("----numOfClients:%d\n", numOfClients);
      System.out.printf("----rowsPerRequest:%d\n", rowsPerRequest);
      try { 
        // begin to insert data 
        System.out.printf("----begin to insert data\n");
	      long startTime = System.currentTimeMillis();
	      int a = numOfFiles/numOfClients;
	      int b = numOfFiles%numOfClients;
	      int last = 0;
	    
	      WriteThread[] writethreads = new WriteThread[numOfClients];
	      int[] wargs = new int[2]; // data file start, end
	      wargs[0] = numOfRows; //rows to be read from each file
        wargs[1] = rowsPerRequest;
        int fstart =0;
        int fend   =0;
	      for (int i = 0; i<numOfClients; ++i) {
	        if (i<b) {
	          fstart = last;
	          fend = last+a;
	          last     = last+a+1;
	          writethreads[i] = new WriteThread(fstart,fend,wargs,datadir);
	          System.out.printf("----Thread %d begin to write\n",i);
	          writethreads[i].start();
	        } else {
	          fstart = last;
	          fend = last+a-1;
	          last     = last+a;  
	          writethreads[i] = new WriteThread(fstart,fend,wargs,datadir);
	          System.out.printf("----Thread %d begin to write\n",i);
	          writethreads[i].start();
	        }
	      } 
	      for (int i =0; i<numOfClients; ++i) {
	        try {
	          writethreads[i].join();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
	      }
	      long stopTime = System.currentTimeMillis();
	      float elapseTime = stopTime - startTime;
	      elapseTime = elapseTime/1000;
	      float speeds  = numOfRows*numOfFiles/elapseTime;
	      System.out.printf("---- insertation speed: %f Rows/Second\n",speeds);
      } catch (Exception ex) {
          ex.printStackTrace();
          System.exit(1);
      } finally {
          System.out.printf("---- insertion end\n");
      }
  
    // above:write part; below: read part;
    } else {
      try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
        String filter_reg;
        String  get_url;
        long startTime;
        long stopTime;
        float elapseTime;
        CloseableHttpResponse responseBody;
        StringEntity stringEntity;
        HttpPost httpPost;
        String qjson;
        for (int ig = 10; ig <110; ig = ig+10) {
          if (ig == 10) {
            filter_reg = "\\b[0-9]\\b";
          } else {
            filter_reg = "\\b" + "([0-9]|" 
              + "[" + "1" + "-" 
              + Integer.toString(ig/10-1) + "][0-9])" +"\\b";
          }
          switch (sqlchoice) {
            case "q1":
              get_url = "http://127.0.0.1:4242/api/query?";
              /*
              get_url = get_url + "start=1563249700&m=none:temperature{devgroup=";
              get_url = get_url + String.valueOf(ig-10) +"}";
              */
              startTime = System.currentTimeMillis();
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"none\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"tags\": {\n" +
              "            \"devgroup\":  " + "\"" + Integer.toString(ig-10) + "\"" + "\n" +
              "       }\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              /*
              System.out.println(responseBody.getStatusLine());
              System.out.println(qjson);
              */
              responseBody.close();
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to get data when devgroup = %d\n",elapseTime, ig-10);
              break; 
            case "q2":
              //count
              startTime = System.currentTimeMillis();
              get_url = "http://127.0.0.1:4242/api/query?";
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"count\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupby\": false\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to count data when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              //avg
              startTime = System.currentTimeMillis();
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"avg\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupby\": false\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to avg data when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              //sum
              startTime = System.currentTimeMillis();
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"sum\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\":  [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" +",\n" +
              "           \"groupby\": false\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to sum data when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              //max
              startTime = System.currentTimeMillis();
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"max\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\":  [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupby\": false\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to max data when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              //min
              startTime = System.currentTimeMillis();
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"min\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\":  [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupby\": false\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              responseBody.close();
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to min data when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              break;
            case "q3":
              startTime = System.currentTimeMillis();
              get_url = "http://127.0.0.1:4242/api/query?";
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"count\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupBy\": true\n" +
              "         }\n" +
              "        ]\n" +
              "     },\n" +
              "     {\n" +
              "       \"aggregator\": \"sum\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupBy\": true\n" +
              "         }\n" +
              "        ]\n" +
              "     },\n" +
              "     {\n" +
              "       \"aggregator\": \"avg\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupBy\": true\n" +
              "         }\n" +
              "        ]\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              /*
              System.out.println(responseBody.getStatusLine());
              System.out.println(qjson);
              */
              stopTime = System.currentTimeMillis();
	            elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to group data by devgroup when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              break;
            case "q4":
              startTime = System.currentTimeMillis();
              get_url = "http://127.0.0.1:4242/api/query?";
              httpPost = new HttpPost(get_url);
              qjson = "  {\n" +
              "   \"start\": 1563249700,\n" +
              "   \"queries\":  [\n" +
              "     {\n" +
              "       \"aggregator\": \"none\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupBy\": false\n" +
              "         }\n" +
              "        ],\n" +
              "       \"downsample\": \"1m-sum\"\n" +
              "     },\n" +
              "     {\n" +
              "       \"aggregator\": \"none\",\n" +
              "       \"metric\": \"temperature\",\n" +
              "       \"filters\": [\n"+
              "         {\n" +
              "           \"type\": \"regexp\",\n" +
              "           \"tagk\": \"devgroup\",\n" +
              "           \"filter\": " +"\"" + filter_reg +"\"" + ",\n" +
              "           \"groupBy\": false\n" +
              "         }\n" +
              "        ],\n" +
              "       \"downsample\": \"1m-avg\"\n" +
              "     }\n" +
              "    ]\n" +
              "  }";
              httpPost.setHeader("Accept", "application/json");
              httpPost.setHeader("Content-type", "application/json");
              stringEntity = new StringEntity(qjson);
              httpPost.setEntity(stringEntity);
              responseBody = httpclient.execute(httpPost);
              /*
              System.out.println(responseBody.getStatusLine());
              System.out.println(qjson);
              */
              stopTime = System.currentTimeMillis();
              elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("Spend %f seconds to group data by time when devgroup < %d\n",elapseTime, ig);
              responseBody.close();
              break;
          }

        }
      httpclient.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("query end:----\n");
    } // end write or query
    System.exit(0);
  }// end main
}// end class
