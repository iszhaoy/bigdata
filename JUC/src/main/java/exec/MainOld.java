package exec;

import java.util.concurrent.BlockingQueue;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 14:45
 */
public class MainOld {
    public static void main(String[] args) throws InterruptedException {
        FileToBlock.fileToBlock();
        BlockingQueue<FileSplit> splitQueue = GlobalEnv.getSplitQueue();
        Count count = new Count();
        System.out.println("开始分发");
        long start = System.currentTimeMillis();
        while(!splitQueue.isEmpty()) {
            FileSplit fs = splitQueue.take();
            Thread thread = new Thread(new MapperRunnerBak(fs, count));
            thread.start();
            thread.join();
        }
        System.out.println("所有线程总共处理了" + count.count + "条");
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000 + "s");
    }
}
