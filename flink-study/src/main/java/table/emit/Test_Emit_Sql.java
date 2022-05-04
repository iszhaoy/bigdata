package table.emit;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Test_Emit_Sql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Table table = tEnv.fromDataStream(
                env.addSource(new SourceData()), "generate_time, name, city, id, event_time.proctime");
        tEnv.createTemporaryView("person", table);

        String emit =
                "SELECT ts / (5 * 60 * 1000),* FROM ( " +
                "SELECT name, COUNT(id),COUNT(DISTINCT id),cast(TUMBLE_START(event_time, interval '10' second) as bigint) as ts " +
                        "FROM person " +
                        "GROUP BY TUMBLE(event_time, interval '10' second), name) " +
                "GROUP BY ts/(5 * 60 * 1000)";
        System.out.println(emit);
        Table result = tEnv.sqlQuery(emit);
        tEnv.toRetractStream(result, Row.class).print();

        env.execute("IncrementalGrouping");
    }

    private static final class SourceData implements SourceFunction<Tuple4<Long, String, String, Long>> {
        @Override
        public void run(SourceContext<Tuple4<Long, String, String, Long>> ctx) throws Exception {
            while (true) {
                long time = System.currentTimeMillis();
                ctx.collect(Tuple4.of(time, "flink", "bj", 1L));
            }
        }

        @Override
        public void cancel() {

        }
    }
}
