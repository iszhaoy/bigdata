package common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class FlinkSettings {
    private static final String checkpointPath = "file:///opt/workspace/work/tmp/checkpoints";

    public static void setjobRestartStrategy(StreamExecutionEnvironment env) {
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
    }

    /**
     * checkpoint
     *
     * @param env
     */
    public static void setjobCheckpoint(StreamExecutionEnvironment env) {
        env.setStateBackend(new FsStateBackend(checkpointPath));
        // 启用检查点，指定触发checkpoint的时间间隔（单位：毫秒，默认500毫秒），默认情况是不开启的
        env.enableCheckpointing(5000L);
        // 设定语义模式，默认情况是exactly_once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设定Checkpoint超时时间，默认为10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多，
        // 最终Flink应用密切触发Checkpoint操作，会占用了大量计算资源而影响到整个应用的性能（单位：毫秒）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 默认情况下，只有一个检查点可以运行
        // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 外部检查点
        // 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务进行恢复
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 设置可以允许的checkpoint失败数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    }
}
