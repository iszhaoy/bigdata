package ThreadPool_;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaoyu
 * @date 2020/6/3
 */
public class TestScheduledThreadPool {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 每隔1秒启动一个线程
        //ScheduledExecutorService pool = Executors.newScheduledThreadPool(5);
        //
        //for (int i = 0; i < 5; i++) {
        //    Future<Integer> result = pool.schedule(new Callable<Integer>() {
        //        @Override
        //        public Integer call() throws Exception {
        //            int num = new Random().nextInt(100);
        //            return num;
        //        }
        //    }, 1, TimeUnit.SECONDS);
        //    System.out.println(result.get());
        //}
        //pool.shutdown();


        // 定时调度线程
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("调度");
            }
        }, 0, 2, TimeUnit.SECONDS);
        //scheduledExecutorService.shutdown();
    }
}
