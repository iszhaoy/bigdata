package com.iszhaoy.utils;

import com.iszhaoy.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 1. 创建命名空间
 * 2. 判断表是否存在
 * 3. 创建表（三张表）
 * <p>
 * 此工具类并不是像是业务线上的，不需要写成连接池方式，每个方法内获取连接并关闭即可
 */
public class HbaseUtils {

    // 创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {

        // 1. 获取Connection
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2. 获取Admin
        Admin admin = connection.getAdmin();

        // 3. 构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        // 4. 创建命名空间
        admin.createNamespace(namespaceDescriptor);

        // 5. 关闭资源
        admin.close();
        connection.close();

    }

    // 判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {

        // 1. 获取Connection
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2. 获取Admin
        Admin admin = connection.getAdmin();

        // 3. 判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        // 4. 关闭资源
        admin.close();
        connection.close();

        // 返回结果
        return exists;
    }

    // 创建表
    public static void createTable(String tablenName, int version, String... cfs) throws IOException {

        // 1. 判断是否传入了列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }

        // 2.判断表是否存在
        if (isTableExist(tablenName)) {
            System.out.println("表已经存在");
            return;
        }
        // 3. 获取Connection
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 4. 获取admin对象
        Admin admin = connection.getAdmin();

        // 5. 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablenName));

        // 6. 循环添加列族信息
        for (String cf : cfs) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 7.设置版本
            hColumnDescriptor.setMaxVersions(version);

            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        // 8. 创建表操作
        admin.createTable(hTableDescriptor);

        // 9. 关闭资源
        admin.close();
        connection.close();
    }

}
