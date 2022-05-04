package api.trigger;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .timeWindowAll(Time.seconds(10))
                .trigger(new CountAndTimeTrigger(TimeCharacteristic.ProcessingTime, 5))
                .process(new ProcessAllWindowFunction<String, Object, TimeWindow>() {

                    private static final long serialVersionUID = -79181324931240382L;

                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<Object> out) throws Exception {
                        for (String element : elements) {
                            out.collect(element);
                        }
                    }
                }).print();

        env.execute();
    }
}
