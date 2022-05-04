package ThreadPool_;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.corba.se.spi.orbutil.threadpool.ThreadPool;

import java.util.concurrent.*;

public class ThreadPoolDemo {
    public static void main(String[] args) {

    }


}

class ThreadPoolUtils {
    /*
      创建固定线程池的方法  核心线程数和最大线程数相同
    *  public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                                      0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<Runnable>());
    *
    * */
    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("iszhaoy" + "-%d").setDaemon(true).build();

    public static ExecutorService createFixedThreadPool() {
        int coreSize = 5;
        int queueSize = 10;
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(coreSize, coreSize, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize), threadFactory, new ThreadPoolExecutor.AbortPolicy());
        return poolExecutor;
        //上面代码是创建一个 5 个线程的固定数量线程池，这里线程存活时间没有作用，所以设置为 0，使用了 ArrayBlockingQueue 作为等待队列，设置长度为 10 ，最多允许10个等待任务，
        // 超过的任务会执行默认的 AbortPolicy 策略，也就是直接抛异常。ThreadFactory 使用了 Guava 库提供的方法，定义了线程名称，方便之后排查问题。
    }

    /*
    * 创建单个线程   核心线程和最大线程都是1  使用了链表阻塞队列
    *  public static ExecutorService newSingleThreadExecutor() {
        return new FinalizableDelegatedExecutorService
            (new ThreadPoolExecutor(1, 1,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>()));
    }
    * */

    /*
    * 创建缓存型线程
    *     public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
                return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(), // 使用了SynchronousQueue,说明队列中不会保存任务,新任务进来后直接创建临时线程处理
                                      threadFactory);
    *
    *
    * 缓存型线程池，在核心线程达到最大值之前，有任务进来就会创建新的核心线程，并加入核心线程池，即时有空闲的线程，也不会复用。
    * 达到最大核心线程数后，新任务进来，如果有空闲线程，则直接拿来使用，如果没有空闲线程，则新建临时线程。并且线程的允许空闲时间都很短，如果超过空闲时间没有活动，则销毁临时线程。
    * 关键点就在于它使用 SynchronousQueue 作为等待队列，它不会保留任务，新任务进来后，直接创建临时线程处理，这样一来，也就容易造成无限制的创建线程，造成 OOM。
    * */

    public static ExecutorService createNewCacheThreadPool() {
        int coreSize = 10;
        int maxSize = 10;

        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(coreSize, maxSize, 0L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), threadFactory, new ThreadPoolExecutor.AbortPolicy());
        return poolExecutor;

    }

    /*
    * 创建调度型现场池
    * 计划型线程池，可以设置固定时间的延时或者定期执行任务，同样是看线程池中有没有空闲线程，如果有，直接拿来使用，如果没有，则新建线程加入池。
    * 使用的是 DelayedWorkQueue 作为等待队列，这中类型的队列会保证只有到了指定的延时时间，才会执行任务。
    * */

    public static ExecutorService createScheduledThreadPool() {
        // TODO
        return null;
    }

}
