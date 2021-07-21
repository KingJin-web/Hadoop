package com.king.hive;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-21 21:05
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


//启动服务:  hive --service hiveserver2&

//异常:
/*
 * Exception in thread "main" java.sql.SQLException: Could not open client transport with JDBC Uri: jdbc:hive2://node3:10000/default: Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: root is not allowed to impersonate root
	at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:224)
	at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107)
	at java.sql.DriverManager.getConnection(DriverManager.java:664)
	at java.sql.DriverManager.getConnection(DriverManager.java:247)
	at com.yc.hive.project.hivePro1.App.main(App.java:26)
 *
 */


/*
 * 通过httpfs协议访问rest接口，以root用户包装自己用户的方式操作HDFS

首先需要开启rest接口，在hdfs-site.xml文件中加入：

<property>
<name>dfs.webhdfs.enabled</name>
<value>true</value>
</property>

然后在core-site.xml文件中加入：
<property>
<name>hadoop.proxyuser.root.hosts</name>
<value>*</value>
</property>
<property>
<name>hadoop.proxyuser.root.groups</name>
<value>*</value>
</property>
 *
 *
 */
public class App {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // replace "hive" here with the name of the user the queries should run
        // as
        Connection con = DriverManager.getConnection("jdbc:hive2://node3:10000/default", "root", "");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string) row format delimited fields terminated by ','  lines terminated by '\n'");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // load data into table
        String filepath = "/king/a.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
