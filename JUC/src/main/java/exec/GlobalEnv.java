package exec;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 14:29
 */
public class GlobalEnv {
    public static final long BLOCK_SIZE = 300000L;
    public static final long BATCH_SIZE = 1000;

    private static BlockingQueue<FileSplit> splitQueue=new LinkedBlockingQueue<>();
    private static List<FileSplit> splitList =new LinkedList<>();

    public static BlockingQueue<FileSplit> getSplitQueue() {
        return splitQueue;
    }

    public static void setSplitQueue(BlockingQueue<FileSplit> splitQueue) {
        GlobalEnv.splitQueue = splitQueue;
    }

    public static List<FileSplit> getSplitList() {
        return splitList;
    }

    public static void setSplitList(List<FileSplit> splitList) {
        GlobalEnv.splitList = splitList;
    }
}
