import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.*;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.math.*;
import java.lang.reflect.Method;


public class WriteThread extends Thread {

  private int[] wargs; // fstart, fend, rows to be read, rows perrequest
  private String fdir;
  private int fstart;
  private int fend;

  public WriteThread (int fstart, int fend,int[] wargs, String fdir) {
    this.fstart = fstart;
    this.fend  = fend;
    this.fdir  = fdir;
    this.wargs = wargs;
  }

  // begin to insert in this thread
  public void run() { 
    StringEntity stringEntity;
    String port = "4242";
    String put_url = "http://127.0.0.1:"+port+"/api/put?summary";
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      /*
      httpclient.getHttpConnectionManager().getParams()
                .setConnectionTimeout(1000);
      httpclient.getHttpConnectionManager().getParams()
                .setSoTimeout(5000);
      */
      for (int i=fstart; i<=fend; i++) {
        String csvfile;
        csvfile = fdir + "/testdata"+ Integer.toString(i)+".csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = " ";
        try {
          br = new BufferedReader(new FileReader(csvfile));
          System.out.println("---- begin to read file " +csvfile+"\n");
          for (int itotalrow =0; itotalrow<wargs[0]; itotalrow=itotalrow+wargs[1]) {
            HttpPost httpPost = new HttpPost(put_url);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            String totaljson = "[\n";
            for (int irow =0; irow<wargs[1]; ++irow) {
              line = br.readLine();
              if (line !=null) {
                String[] meter = line.split(cvsSplitBy);
                // devid, devname,devgroup,ts,temperature,humidity
                BigInteger timestamp = new BigInteger(meter[3]);
                timestamp = timestamp.divide(BigInteger.valueOf(1000));
                long ts   = timestamp.longValue();
                int temperature = Integer.parseInt(meter[4]);
                float humidity = Float.parseFloat(meter[5]);
                String onejson = "  {\n" +
                    "   \"metric\": \"temperature\",\n" +
                    "   \"timestamp\":  " + String.valueOf(ts) + ",\n" +
                    "   \"value\":  " + String.valueOf(temperature) + ",\n" +
                    "   \"tags\" : {\n" +
                    "      \"devid\":" +" \"" + meter[0] + "\",\n" +
                    "      \"devname\":" +" \"" + meter[1] + "\",\n" +
                    "      \"devgroup\":" +" \"" + meter[2] + "\"\n" +
                    "    }\n" +
                    "   },\n" +
                    "  {\n" +
                    "   \"metric\": \"humidity\",\n" +
                    "   \"timestamp\":  " + String.valueOf(ts) + ",\n" +
                    "   \"value\":  " + String.valueOf(humidity) + ",\n" +
                    "   \"tags\" : {\n" +
                    "      \"devid\":" +" \"" + meter[0] + "\",\n" +
                    "      \"devname\":" +" \"" + meter[1] + "\",\n" +
                    "      \"devgroup\":" +" \"" + meter[2] + "\"\n" +
                    "    }\n";
                if (irow == 0) {
                  totaljson = totaljson + onejson;
                } else if (irow < wargs[1]) {
                  totaljson = totaljson + " },\n" + onejson;
                }
              } //end one line reading
            } //end on batch put
            totaljson = totaljson + " }\n]";
            stringEntity = new StringEntity(totaljson);
            httpPost.setEntity(stringEntity);
            CloseableHttpResponse responseBody = httpclient.execute(httpPost);
            /*
            System.out.println(responseBody.getStatusLine());
            System.out.println(totaljson);
            */
            responseBody.close();            
          }// end one file reading
        } catch (FileNotFoundException e) {
        e.printStackTrace();
        } catch (IOException e) {
        e.printStackTrace();
        } finally {
          if (br != null) {
            try {
              br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
          }
        } 
      }//end file iteration
      httpclient.close();
    } catch (Exception e) {
        e.printStackTrace();
        System.out.println("failed to connect");
    }
  }//end run
}//end class
