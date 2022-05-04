package table.emit;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Test_Emit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        /*  现在Emit的原理是这样子的：
        - *当某个key*下面来了第一条数据的时候，注册一个emit delay之后的*处理时间定时器*；
        - 当定时器到了的时候，
        - 检查当前的key下的聚合结果跟上次输出的结果是否有变化，
        - 如果有变化，就发送-[old], +[new] 两条结果到下游；
        - 如果是*没有变化，则不做任何处理*；
        - 再次注册一个新的emit delay之后的处理时间定时器。*/
        // 官方文档并未写出，实验性功能
        tEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        // 每隔一秒输出一次结果
        tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "1000 ms");

        Table table = tEnv.fromDataStream(
                env.addSource(new SourceData()), "generate_time, name, city, id, event_time.rowtime");
        tEnv.createTemporaryView("person", table);

        String emit =
                "SELECT name, COUNT(id) as cnt,COUNT(DISTINCT id) as cnt_distinct " +
                        "FROM person " +
                        "GROUP BY HOP(event_time, interval '10' second), name";

        Table result = tEnv.sqlQuery(emit);
        tEnv.toRetractStream(result, Row.class).print();

        env.execute("IncrementalGrouping");
    }

    private static final class SourceData implements SourceFunction<Tuple4<Long, String, String, Long>> {
        @Override
        public void run(SourceContext<Tuple4<Long, String, String, Long>> ctx) throws Exception {
            while (true) {
                long time = System.currentTimeMillis();
                Thread.sleep(1000);
                ctx.collect(Tuple4.of(time, "flink", "bj", 1L));
            }
        }

        @Override
        public void cancel() {

        }
    }
}
