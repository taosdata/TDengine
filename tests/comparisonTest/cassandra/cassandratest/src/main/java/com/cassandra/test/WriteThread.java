import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.session.*;
import com.datastax.oss.driver.api.core.config.*;


public class WriteThread extends Thread {

  private int[] wargs; // fstart, fend, rows to be read, rows perrequest
  private String fdir;
  private int fstart;
  private int fend;
  private boolean q4flag;

  public WriteThread (int fstart, int fend,int[] wargs, String fdir, boolean q4flag) {
    this.fstart = fstart;
    this.fend  = fend;
    this.fdir  = fdir;
    this.wargs = wargs;
    this.q4flag = q4flag;
  }

  // begin to insert in this thread
  public void run() {
    /*
    // this configuration file makes sure no timeout error
    File confile = new File("/home/ubuntu/fang/cassandra/application.conf");
    */
    // connect to server
    try {
      CqlSession session = CqlSession.builder()
          //.withConfigLoader(DriverConfigLoader.fromFile(confile))
          .build();
      //session.execute("use cassandra");
      int tominute = 6000;
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
            String cqlstr = "BEGIN BATCH ";
            for (int irow =0; irow<wargs[1]; ++irow) {
              line = br.readLine();
              if (line !=null) {
                String[] meter = line.split(cvsSplitBy);
                BigInteger tminute = new BigInteger(meter[3]);
                tminute = tminute.divide(BigInteger.valueOf(tominute));
                if (q4flag) {
                  cqlstr = cqlstr + "insert into cassandra.test (devid,devname,devgroup,ts, minute,temperature,humidity) values ";
                  cqlstr = cqlstr +"("+meter[0] +"," +"'" +meter[1] +"'" +"," +meter[2] +"," + meter[3] +",";
                  cqlstr = cqlstr +tminute.toString() +"," +meter[4] +"," +meter[5] +");";
                } else {
                  cqlstr = cqlstr + "insert into cassandra.test (devid,devname,devgroup,ts,temperature,humidity) values ";
                  cqlstr = cqlstr +"("+meter[0] +"," +"'" +meter[1] +"'" +"," +meter[2] +"," + meter[3] +",";
                  cqlstr = cqlstr +meter[4] +"," +meter[5] +");";
               }
             } // if this line is not null
            }//end row iteration in one batch
            cqlstr = cqlstr+" APPLY BATCH;";
            try {
              //System.out.println(cqlstr+"----\n");
              session.execute(cqlstr);
            } catch (Exception ex) {
              ex.printStackTrace();
            } 

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
      session.close();

    } catch (Exception ex) {
        ex.printStackTrace();
    }
  }//end run
}//end class
