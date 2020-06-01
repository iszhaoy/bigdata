package ThreadPool_;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author zhaoyu
 * @date 2020/6/3
 * 线程池： 提供了一个线程队列，队列中保存着所有等待状态的线程，避免了创建与销毁额外的开销，提高了响应速度
 * <p>
 * 线程池的体系结构：
 * java.util.concurrent.Executor ： 负责线程的使用和调度的根接口
 * |-- ExecutorService: 线程池的主要接口
 * |-- ThreadPoolExecutor 线程池实现类
 * |-- ScheduledExecutorService 子接口： 负责线程调度
 * |-- ScheduledThreadPoolExecutor： 继承了ThreadPoolExecutor并 实现了ScheduledExecutorService
 * <p>
 * <p>
 * ExecutorService newFixedThreadPool() ： 创建固定大小的线程池
 * ExecutorService newCachedThreadPool() ： 缓存线程池，线程池的数据不固定，可以根据需求自动的更改数量
 * ExecutorService newSingleThreadPoolExecutor() ：创建单个线程池，线程池中只有一个线程
 * <p>
 * ScheduledExecutorService newScheduledThreadPool() : 创建固定大小的线程，可以延迟或者定时的执行任务
 */
public class TestThreadPool {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        MyThread thread = new MyThread();

        //for (int i = 0; i < 5; i++) {
        //    executorService.submit(thread);
        //}

        List<Future<Integer>> list = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Future<Integer> future = executorService.submit(new Callable<Integer>() {
                int i = 0;

                @Override
                public Integer call() throws Exception {

                    for (int j = 0; j < 10; j++) {
                        i++;
                    }

                    return i;
                }
            });
            list.add(future);
        }

        executorService.shutdown();

        for (Future<Integer> future : list) {
            System.out.println(future.get());
        }
    }
}



class MyThread implements Runnable {

    int i = 0;

    @Override
    public void run() {
        for (int j = 0; j < 100; j++) {
            System.out.println(Thread.currentThread().getName() + " i:" + i++);

        }
    }
}