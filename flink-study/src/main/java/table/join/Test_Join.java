package table.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class Test_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple3<String, String, String>> orderData = new ArrayList<>();
        orderData.add(Tuple3.of("oid001", "US Dollar", "2021-01-10 12:00:00"));
        orderData.add(Tuple3.of("oid002", "Euro", "2021-01-10 12:01:00"));
        orderData.add(Tuple3.of("oid003", "Yen", "2021-01-10 12:02:00"));
        orderData.add(Tuple3.of("oid001", "US Dollar", "2021-01-10 12:02:00"));
        //orderData.add(Tuple3.of("oid004", "Euro", "2021-01-10 12:05:05"));

        DataStream<Tuple3<String, String, Long>> orderDataStream = env.fromCollection(
                orderData)
                .map((o) -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long time = sdf.parse(o.f2).getTime();
                    return new Tuple3<>(o.f0, o.f1, time + 28800000L);
                }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>)
                                        (element, recordTimestamp) -> element.f2
                                )
                );

        // 提供一个汇率历史记录表静态数据集
        List<Tuple3<String, Long, String>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple3.of("US Dollar", 102L, "2021-01-10 12:00:05"));
        ratesHistoryData.add(Tuple3.of("Euro", 114L, "2021-01-10 12:00:59"));
        ratesHistoryData.add(Tuple3.of("Yen", 1L, "2021-01-10 12:01:59"));
        ratesHistoryData.add(Tuple3.of("Euro", 116L, "2021-01-10 12:01:59"));
        //ratesHistoryData.add(Tuple3.of("Euro", 119L, "2021-01-10 12:00:32"));

        // 用上面的数据集创建并注册一个示例表，在实际设置中，应使用自己的表替换它
        DataStream<Tuple3<String, Long, Long>> ratesHistoryStream = env
                .fromCollection(ratesHistoryData)
                .map((r) -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long time = sdf.parse(r.f2).getTime();
                    return new Tuple3<>(r.f0, r.f1, time + 28800000L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Tuple3<String, Long, Long>>)
                                                (element, recordTimestamp) -> element.f2
                                )
                );


        Table orders = tEnv.fromDataStream(
                orderDataStream,
                $("id"),
                $("currency"),
                $("rowtime").rowtime(),
                $("proctime").proctime()
        );
        Table ratesHistory = tEnv.fromDataStream(
                ratesHistoryStream,
                $("currency"),
                $("rate"),
                $("rowtime").rowtime()
        );

        tEnv.createTemporaryView("Orders", orders);
        tEnv.createTemporaryView("Rates", ratesHistory);

        // 常规join
        //String query = "SELECT o.id,r.rate FROM Orders o LEFT JOIN Rates r on o.currency = r.currency";
        //tEnv.toAppendStream(tEnv.sqlQuery(query), Row.class).print();
        //tEnv.toRetractStream(tEnv.sqlQuery(query), Row.class).print();

        // interval join
        String query = "SELECT o.id,r.rate,o.rowtime FROM Orders o LEFT JOIN Rates r on o.currency = r.currency " +
                " and r.rowtime between o.rowtime - interval '1' minute and o.rowtime + interval '1' minute";
        tEnv.toRetractStream(tEnv.sqlQuery(query), Row.class).print();

        env.execute("RegularJoin");
    }
}
