package com.taos.test;

import com.taos.example.CloudTutorial;
import com.taos.example.ConnectCloudExample;
import org.junit.Test;

import java.sql.SQLException;

public class TestAll {

    @Test
    public void testConnectCloudExample() throws SQLException {
        ConnectCloudExample.main(new String[]{});
    }

//    @Test
//    public void testCloudTutorial() throws SQLException {
//        CloudTutorial.main(new String[]{});
//    }
}
