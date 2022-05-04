package table.catalogs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test_Catalogs {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String show_catalogs = "show catalogs";
        String show_databases = "show databases";
        String show_tables = "show tables";
        String show_funcations = "show functions";
        tEnv.executeSql(show_catalogs).print();
        tEnv.executeSql(show_databases).print();
        tEnv.executeSql(show_tables).print();
        tEnv.executeSql(show_funcations).print();
    }
}
