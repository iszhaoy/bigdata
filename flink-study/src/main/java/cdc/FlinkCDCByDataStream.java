package cdc;

import cdc.func.MyDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCByDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.enableCheckpointing(5000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc_test/ck"));
        //能在flink 1.12  1.13使用 监控多库多表
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("cdc_test")
                .tableList("cdc_test.*")
                //.deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.print();

        env.execute("CDC");

    }
}
