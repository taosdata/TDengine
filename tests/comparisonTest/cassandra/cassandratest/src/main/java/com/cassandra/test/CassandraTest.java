import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.session.*;
import com.datastax.oss.driver.api.core.config.*;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Cluster;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;
import java.math.*;
import java.lang.reflect.Method;


public class CassandraTest{
  
  public static void main(String args[]) {
    
     
    // begin to parse argument
    String datadir = "/home/ubuntu/testdata";
    String sqlfile  = "/home/ubuntu/fang/cassandra/q1.txt";
    String cfgfile = "/home/ubuntu/fang/cassandra/application.conf"; 
    boolean q4flag = false;
    int numOfRows = 1000000;
    int numOfFiles =0;
    int numOfClients =0;
    int rowsPerRequest =0;
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
                sqlfile = args[++i];
        } else if (args[i].equalsIgnoreCase("-timetest")) {
                q4flag = true;
        } else if (args[i].equalsIgnoreCase("-conf")) {
                cfgfile = args[++i];
        }
    }
    // file below to make sure no timeout error
    File confile = new File(cfgfile);

    System.out.println("parameters\n");

    if (numOfFiles >0) {
      // write data
      System.out.printf("----dataDir:%s\n", datadir);
      System.out.printf("----numOfFiles:%d\n", numOfFiles);
      System.out.printf("----numOfClients:%d\n", numOfClients);
      System.out.printf("----rowsPerRequest:%d\n", rowsPerRequest);

      // connect to cassandra server
      System.out.printf("----connecting to cassandra server\n");
      try {
        CqlSession session = CqlSession.builder()
          .withConfigLoader(DriverConfigLoader.fromFile(confile))
          .build();
        
        session.execute("drop keyspace if exists cassandra");
        session.execute("CREATE KEYSPACE if not exists cassandra WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}");
        if (q4flag) {
          session.execute("create table if not exists cassandra.test (devid int, devname text, devgroup int, ts bigint, minute bigint, temperature int, humidity float ,primary key (minute,ts,devgroup,devid,devname))");
        } else {
          session.execute("create table if not exists cassandra.test (devid int, devname text, devgroup int, ts bigint, temperature int, humidity float ,primary key (devgroup,devid,devname,ts))");
        }
        session.close();
        System.out.printf("----created keyspace cassandra and table test\n");
        
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
	          writethreads[i] = new WriteThread(fstart,fend,wargs,datadir,q4flag);
	          System.out.printf("----Thread %d begin to write\n",i);
	          writethreads[i].start();
	        } else {
	          fstart = last;
	          fend = last+a-1;
	          last     = last+a;  
	          writethreads[i] = new WriteThread(fstart,fend,wargs,datadir,q4flag);
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
      // query data begin
      System.out.printf("----sql command file:%s\n", sqlfile);
      // connect to cassandra server
      try {
        CqlSession session = CqlSession.builder()
          .withConfigLoader(DriverConfigLoader.fromFile(confile))
          .build();

        //session.execute("use cassandra;");
        BufferedReader br = null;
        String line = "";
        try {
          br = new BufferedReader(new FileReader(sqlfile));
          while ((line = br.readLine()) != null && line.length()>10) {
            long startTime = System.currentTimeMillis();
            // begin to query one line command //
            // end querying one line command
            try {

              ResultSet results = session.execute(line);
              long icounter = 0;
              for (Row row : results) {
                  icounter++;
              }

              long stopTime = System.currentTimeMillis();
              float elapseTime = stopTime - startTime;
              elapseTime = elapseTime/1000;
              System.out.printf("----spend %f seconds to query: %s\n", elapseTime, line);
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.printf("---- query failed!\n");
                System.exit(1);
            }
  
          }
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
          session.close();
        }
      } catch (Exception ex) {
          ex.printStackTrace();
      } finally {
          System.out.println("query end:----\n");
      }
    } // end write or query
    System.exit(0);
  }// end main
}// end class
