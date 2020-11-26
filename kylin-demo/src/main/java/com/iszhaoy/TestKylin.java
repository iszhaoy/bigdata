package com.iszhaoy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestKylin {
    public static void main(String[] args) throws Exception {
        // kylin 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        // //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://bigdata01:7070/gmall";

        //Kylin 的用户名
        String KYLIN_USER = "ADMIN";
        //Kylin 的密码
        String KYLIN_PASSWD = "KYLIN";

        // 添加驱动信息
        Class.forName(KYLIN_DRIVER);

        // 获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);


        // 预编译 SQL
        PreparedStatement ps = connection.prepareStatement(
                "SELECT  p.region_name \n" +
                        "       ,SUM(payment_amount)\n" +
                        "FROM DWD_FACT_PAYMENT_INFO v\n" +
                        "LEFT JOIN DWD_DIM_BASE_PROVINCE p\n" +
                        "ON v.province_id = p.id\n" +
                        "GROUP BY  p.region_name ");

        // 执行查询
        ResultSet resultSet = ps.executeQuery();

        //遍历打印
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getLong(2));
        }
    }
}
