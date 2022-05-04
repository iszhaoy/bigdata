package api.trigger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Preconditions;

@Slf4j
public class CountAndTimeTrigger extends Trigger<Object, TimeWindow> {

    private static final long serialVersionUID = -2846888736149730999L;


    private final TimeCharacteristic timeType;
    private final Long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    public CountAndTimeTrigger(TimeCharacteristic timeType, long maxCount) {
        super();
        this.timeType = timeType;
        this.maxCount = maxCount;
    }

    private void verify(TimeCharacteristic timeType, long maxCount) {
        Preconditions.checkNotNull(timeType);
        if (maxCount <= 0) throw new IllegalArgumentException();
    }

    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onElement(Object o, long time, TimeWindow timeWindow, TriggerContext ctx) throws Exception {

        if (timeType != TimeCharacteristic.EventTime) {
            ctx.registerProcessingTimeTimer(timeWindow.maxTimestamp());
        }

        if (timeType != TimeCharacteristic.ProcessingTime) {
            ctx.registerEventTimeTimer(timeWindow.maxTimestamp());
        }

        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);

        Long c = count.get();
        if (c >= maxCount) {
            count.clear();
            log.info("fire with count: " + count);
            return fireAndPurge(timeWindow, ctx);
        }

        if (time >= timeWindow.getEnd()) {
            log.info("fire with time: " + count);
            return fireAndPurge(timeWindow, ctx);
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext ctx) throws Exception {

        if (timeType != TimeCharacteristic.ProcessingTime || time >= timeWindow.getEnd()) {
            return TriggerResult.CONTINUE;
        }

        log.info("fire with process tiem: " + time);
        return fireAndPurge(timeWindow, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext ctx) throws Exception {

        if (timeType != TimeCharacteristic.EventTime || time >= timeWindow.getEnd()) {
            return TriggerResult.CONTINUE;
        }

        log.info("fire with process tiem: " + time);
        return fireAndPurge(timeWindow, ctx);
    }

    @Override
    public boolean canMerge() {
        return false;
    }

    @Override
    public void onMerge(TimeWindow timeWindow, OnMergeContext ctx) throws Exception {

        ctx.mergePartitionedState(stateDesc);

        long maxTimestamp = timeWindow.maxTimestamp();

        if (timeType != TimeCharacteristic.EventTime && maxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(maxTimestamp);
        }

        if (timeType != TimeCharacteristic.ProcessingTime && maxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(maxTimestamp);
        }
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();

        if (timeType != TimeCharacteristic.ProcessingTime) {
            ctx.deleteEventTimeTimer(timeWindow.maxTimestamp());
        }

        if (timeType != TimeCharacteristic.EventTime) {
            ctx.deleteProcessingTimeTimer(timeWindow.maxTimestamp());
        }
    }

}

class Sum implements ReduceFunction<Long> {
    private static final long serialVersionUID = 7157804424362855999L;

    @Override
    public Long reduce(Long v1, Long v2) throws Exception {
        return v1 + v2;
    }
}