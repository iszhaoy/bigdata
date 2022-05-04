package cdc;

import com.mysql.cj.protocol.ResultsetRowsOwner;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class FlinkCDCByTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 字段名要和mysql表对应,类型如果可以可以转换会自动转换,转换不了的就会报错
        // Table的方式 只能监控单表 只能在flink 1.13使用
        tEnv.executeSql("CREATE TABLE USER_INFO (\n" +
                " id STRING primary key,\n" +
                " `user` STRING,\n" +
                " sex STRING,\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'root',\n" +
                " 'database-name' = 'cdc_test',\n" +
                " 'table-name' = 'user_info'\n" +
                ")");

        Table user_info = tEnv.sqlQuery("select * from USER_INFO");

        user_info.printSchema();

        // 这里有点问题
        DataStream<Tuple2<Boolean, User>> tuple2DataStream = tEnv.toRetractStream(user_info, User.class);

        tuple2DataStream.print();
        env.execute("CDCByTable");

    }
}


class User implements Serializable {
    private static final long serialVersionUID = 7563739010311621179L;
    private String id;
    private String user;
    private String sex;
    private int age;

    public User() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", user='" + user + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                '}';
    }
}