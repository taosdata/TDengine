package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.io.*;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class NativeLoader {


    public static void main(String[] args) throws IOException {
        InputStream in = NativeLoader.class.getResourceAsStream("libtaos.so");
        System.out.println(in == null);
    }

    public static boolean load() throws Exception {
        return false;
//        String libname = null;
//        if (OSUtils.isLinux()) {
//            libname = "libtaos.so";
//        } else if (OSUtils.isWindows()) {
//            libname = "taos.dll";
//        } else if (OSUtils.isMac()) {
//
//        }
//
//        if (NativeLoader.class.getResource(libname) == null)
//            throw new Exception("error loading native library: " + libname);
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_LOAD_JNI_LIBRARY_FAILED);
//        String tmpFolder = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
//        return extractAndLoadLibraryFile(libname,tmpFolder);
    }

//    private static boolean extractAndLoadLibraryFile(String libname, String tmpFolder) {
//        return false;
//    }


    private static String md5sum(InputStream input) throws IOException {
        BufferedInputStream in = new BufferedInputStream(input);
        try {
            MessageDigest digest = java.security.MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(in, digest);
            for (; digestInputStream.read() >= 0; ) {
            }
            ByteArrayOutputStream md5out = new ByteArrayOutputStream();
            md5out.write(digest.digest());
            return md5out.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm is not available: " + e);
        } finally {
            in.close();
        }
    }

}
