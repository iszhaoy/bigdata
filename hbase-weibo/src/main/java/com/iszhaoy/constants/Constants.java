package com.iszhaoy.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {

    // hbase的配置信息
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();

    // 命名空间
    public static final String NAMESPACE = "weibo";

    // 微博内容表
    public static final String CONTEENT_TABLE = "weibo:content";
    public static final String CONTEENT_TABLE_CF = "info";
    public static final int CONTEENT_TABLE_VERSION = 1;

    // 用户关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final String RELATION_TABLE_CF1 = "attends";
    public static final String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSION = 1;

    // 收件降表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSION = 2;

}
