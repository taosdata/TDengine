/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.taosdata.example;

import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.configuration.JAXPConfigurator;

import java.sql.*;

public class ProxoolDemo {


    public static void main(String[] args) {

        String xml = parseConfigurationXml(args);
        if (xml == null) {
            printHelp();
            System.exit(0);
        }

        try {
            JAXPConfigurator.configure(xml, false);
            Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
            Connection connection = DriverManager.getConnection("proxool.ds");

            Statement stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery("show databases");
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    System.out.print(metaData.getColumnLabel(i) + ": " + rs.getString(i));
                }
                System.out.println();
            }

            stmt.close();

        } catch (ClassNotFoundException | SQLException | ProxoolException e) {
            e.printStackTrace();
        }
    }

    private static String parseConfigurationXml(String[] args) {
        String host = null;
        for (int i = 0; i < args.length; i++) {
            if ("--xml".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
        }
        return host;
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar ProxoolDemo.jar --xml [xml]");
    }

}
