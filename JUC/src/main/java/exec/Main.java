package exec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 14:45
 */
public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        FileToBlock.fileToBlock();
        long start = System.currentTimeMillis();
        List<FileSplit> splitList = GlobalEnv.getSplitList();
        System.out.println("开始分发");

        List<Future<Long>> result = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(7);
        for (FileSplit fs : splitList) {
            System.out.println(fs);
            Future<Long> submit = executorService.submit(new MapperRunner(fs));
            result.add(submit);
        }
        executorService.shutdown();
        long count = result.stream()
                .mapToLong(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return 0L;
                }).sum();

        System.out.println("所有线程总共处理了" + count + "条");
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000 + "s");
    }
}
