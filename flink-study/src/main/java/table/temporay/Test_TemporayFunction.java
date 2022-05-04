package table.temporay;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class Test_TemporayFunction {

    private static final Logger log = LoggerFactory.getLogger(Test_TemporayFunction.class);

    public static void main(String[] args) throws Exception {

        // 获取 stream 和 table 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        // flink 1.12中的默认流时间已经是eventtime，可以不需要显式的设置
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 订单表
        List<Tuple3<String, String, String>> orderData = new ArrayList<>();
        orderData.add(Tuple3.of("oid001", "US Dollar", "2021-01-10 12:00:05"));
        orderData.add(Tuple3.of("oid002", "Euro", "2021-01-10 12:01:08"));
        orderData.add(Tuple3.of("oid003", "Yen", "2021-01-10 12:02:10"));
        orderData.add(Tuple3.of("oid004", "Euro", "2021-01-10 12:05:05"));
        orderData.add(Tuple3.of("oid005", "TEST", "2021-01-10 12:05:05"));

        DataStream<Tuple3<String, String, Long>> orderDataStream = env.fromCollection(
                orderData)
                // 转换时间戳：The rowtime attribute can only replace a field with a valid time type,
                // such as Timestamp or Long. But was: String
                .map((o) -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long time = sdf.parse(o.f2).getTime();
                    return new Tuple3<>(o.f0, o.f1,
                            // 修改时区为东八区
                            time + 28800000L);
                    /*
                     * due to type erasure. You can give type information hints by using the returns(...)
                     * method on the result of the transformation call, or by letting your function
                     * implement the 'ResultTypeQueryable' interface.
                     * */
                }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
        //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(
        //        Time.seconds(1)) {
        //    @Override
        //    public long extractTimestamp(Tuple3<String, String, Long> element) {
        //        return element.f2;
        //    }
        //});
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2));

        orderDataStream.print("time");

        Table order = tEnv.fromDataStream(
                orderDataStream,
                "o_id,o_currency,o_time.rowtime");

        // 注册订单表
        tEnv.createTemporaryView("Orders", order);

        // 提供一个汇率历史记录表静态数据集
        List<Tuple3<String, Long, String>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple3.of("US Dollar", 102L, "2021-01-10 12:00:03"));
        ratesHistoryData.add(Tuple3.of("Euro", 114L, "2021-01-10 12:00:07"));
        ratesHistoryData.add(Tuple3.of("Yen", 1L, "2021-01-10 12:00:30"));
        ratesHistoryData.add(Tuple3.of("Euro", 116L, "2021-01-10 12:00:31"));
        ratesHistoryData.add(Tuple3.of("Euro", 119L, "2021-01-10 12:05:06"));

        // 用上面的数据集创建并注册一个示例表，在实际设置中，应使用自己的表替换它
        DataStream<Tuple3<String, Long, Long>> ratesHistoryStream = env
                .fromCollection(ratesHistoryData)
                .map((r) -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long time = sdf.parse(r.f2).getTime();
                    return new Tuple3<>(r.f0, r.f1, time + 28800000L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.LONG))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Long>>(
                        Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Long> element) {
                        return element.f2;
                    }
                });

        ratesHistoryStream.print();
        Table ratesHistory = tEnv.fromDataStream(
                ratesHistoryStream,
                $("r_currency"),
                $("r_rate"),
                $("r_rowtime").rowtime());

        log.info("注册订单表完场");
        tEnv.createTemporaryView("RatesHistory", ratesHistory);
        log.info("注册汇率表完成");

        //// 创建和注册时态表函数
        //// 指定 "r_rowtime" 为时间属性，指定 "r_currency" 为主键
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
                $("r_rowtime"),
                $("r_currency"));

        tEnv.createTemporarySystemFunction("Rates", rates);

        // 如果要传入TemporalTableFunction事件时间属性，那么定义TemporalTableFunction时，也需要定义成事件时间，
        // 否则会报错：Non processing timeAttribute [TIME ATTRIBUTE(ROWTIME)] passed as the argument to TemporalTableFunction
        // 测试表函数
        String dml = "SELECT * FROM Orders AS o , LATERAL TABLE (Rates(o.o_time)) AS r where r.r_currency=o.o_currency";
        log.info(String.format("执行DDL:%s", dml));
        Table resultTable = tEnv.sqlQuery(dml);
        //
        tEnv.toAppendStream(resultTable, Row.class).print();

        env.execute("TemporayTable");
    }
}
