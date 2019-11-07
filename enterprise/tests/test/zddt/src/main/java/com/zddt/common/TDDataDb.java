package com.zddt.common;

public class TDDataDb {
    public static boolean init() {
        return true;
    }
    public static int createTb(String tableName){return 1;}
    public static synchronized  boolean createStb(){return true;}
    public static boolean checkEmpty(){return true;}
}
