package table.module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class Test_HiveModule {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        System.out.println(Arrays.toString(tEnv.listModules()));
        String hiveNmae = "hive";
        String version = "2.3.6";
        tEnv.loadModule(hiveNmae, new HiveModule(version));

        Arrays.stream(tEnv.listModules()).forEach(System.out::println);
        System.out.println("===========================");
        Arrays.stream(tEnv.listFunctions()).forEach(System.out::println);

        Table table = tEnv.sqlQuery("select date_format(date_add('2020-12-11',1),'yyyy-MM-dd HH:mm:ss'),current_date()");
        tEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
