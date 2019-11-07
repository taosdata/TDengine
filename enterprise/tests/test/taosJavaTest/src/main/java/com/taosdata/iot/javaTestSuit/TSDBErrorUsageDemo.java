package com.taosdata.iot.javaTestSuit;

import com.taosdata.jdbc.TSDBError;

public class TSDBErrorUsageDemo {
    public static void main(String[] args) {
//        com.taosdata.jdbc.TSDBErrorUsageDemo demo = new com.taosdata.jdbc.TSDBErrorUsageDemo();
        int errorCode = 0;

        /////////////////////////////////////////////////
        // error enum
        System.out.println(TSDBError.TSDB_CODE_SUCCESS);
        // get code from error enum
        System.out.println(TSDBError.TSDB_CODE_SUCCESS.getErrCode());
        // get message from error enum
        System.out.println(TSDBError.TSDB_CODE_SUCCESS.getErrMessage());
        // get error enum from error code
        System.out.println(TSDBError.values()[45].getErrMessage());

        /////////////////////////////////////////////////
//        public static TSDBError getErrorFromCode(int errCode) {
//        if ((errCode >= 5 && errCode <= 87) || (errCode >= 0 && errCode <= 1)) {
//            return TSDBError.values()[errCode];
//        } else {
//            return null;
//        }
//    }




        ////////////////////////////////////////////////////////////////
//        File errFile = new File("/home/jyhou/workspace/errtmp");
//        File msgFile = new File("/home/jyhou/workspace/msgtmp");
//
//        try {
//            BufferedReader errReader = new BufferedReader(new FileReader(errFile));
//            BufferedReader msgReader = new BufferedReader(new FileReader(msgFile));
//            String errLine = "";
//            String msgLine = "";
//            while ((errLine = errReader.readLine()) != null) {
//                msgLine = msgReader.readLine().trim().replace(",","),");
//                StringBuilder builder = new StringBuilder(errLine.trim());
//                builder.replace(builder.length()-2, builder.length(),"");
//                builder.append(", ").append(msgLine);
//                System.out.println(builder.toString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


    }
}
