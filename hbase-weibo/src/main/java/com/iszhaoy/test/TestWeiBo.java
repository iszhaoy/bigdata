package com.iszhaoy.test;

import com.iszhaoy.constants.Constants;
import com.iszhaoy.dao.HbaseDao;
import com.iszhaoy.utils.HbaseUtils;

import java.io.IOException;

public class TestWeiBo {

    public static void init() {
        try {
            // 创建命名空间
            HbaseUtils.createNameSpace(Constants.NAMESPACE);

            // 创建微博内容表
            HbaseUtils.createTable(Constants.CONTEENT_TABLE, Constants.CONTEENT_TABLE_VERSION, Constants.CONTEENT_TABLE_CF);

            // 创建用户关系表
            HbaseUtils.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSION, Constants.RELATION_TABLE_CF1,
                    Constants.RELATION_TABLE_CF2);

            // 创建收件箱表
            HbaseUtils.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSION, Constants.INBOX_TABLE_CF);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // 初始化
//        init();

        // 1001发布微博
        HbaseDao.publishWeiBo("1001", "赶紧下课吧！！！");

        // 1002关注1001 和1003
        HbaseDao.addAttends("1002", "1001", "1003");

        // 获取1002初始化页面
        HbaseDao.getInit("1002");

        System.out.println("***********111************");

        // 1003发布3条微博，同时1001发布2条微博
        HbaseDao.publishWeiBo("1003", "谁说的赶紧下课！！！");
        Thread.sleep(10);

        HbaseDao.publishWeiBo("1001", "我没说话！！！");
        Thread.sleep(10);

        HbaseDao.publishWeiBo("1003", "那谁说的！！！");
        Thread.sleep(10);

        HbaseDao.publishWeiBo("1001", "反正飞机是下线了！！！");
        Thread.sleep(10);

        HbaseDao.publishWeiBo("1003", "你们爱咋咋地！！！");

        // 获取1002的初始页面
        HbaseDao.getInit("1002");
        System.out.println("***********222************");

        // 1002 取关1003
        HbaseDao.deleteAttends("1002", "1003");
        // 获取1002的初始页面
        HbaseDao.getInit("1002");
        System.out.println("***********333************");

        // 1002再次关注1003
        HbaseDao.addAttends("1002", "1003");
        HbaseDao.getInit("1002");

        System.out.println("***********444************");

        // 获取1001微博详情
        HbaseDao.getWeiBo("1001");
    }
}
