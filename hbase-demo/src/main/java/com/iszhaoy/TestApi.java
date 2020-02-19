package com.iszhaoy;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Hello world!
 */
public class TestApi {

    public static boolean tableIsExist(String tableName) {
        HBaseConfiguration config = new HBaseConfiguration();
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
        HBaseAdmin admin = null;
        boolean res = false;
        try {
            admin = new HBaseAdmin(config);
            res = admin.tableExists(tableName);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;

    }

    public static void main(String[] args) {
        System.out.println(tableIsExist("cumin:stu"));
    }
}
