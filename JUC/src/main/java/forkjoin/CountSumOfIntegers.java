
package forkjoin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

public class CountSumOfIntegers extends RecursiveTask<Integer> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final int THREADHOLD = 100000000 / 8;
    private int start, end;

    public CountSumOfIntegers(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        long t1 = System.currentTimeMillis();
        int sum = 0;
        for (int i = 0; i <= 100000000; i++)
            sum += i;
        long t2 = System.currentTimeMillis();
        System.out.println("final sum = " + sum);
        System.out.println("TimeUsage with 1 thread: " + (t2 - t1));
        ForkJoinPool fp = new ForkJoinPool();
        CountSumOfIntegers task = new CountSumOfIntegers(1, 100000000);
        Future<Integer> result = fp.submit(task);
        System.out.println("waiting .....");
        System.out.println("final sum = " + result.get());
        System.out.println("TimeUsage with 8 thread: " + (System.currentTimeMillis() - t2));
    }

    @Override
    protected Integer compute() {
        int sum = 0;
        if (end - start <= THREADHOLD) {
            for (int i = start; i <= end; i++)
                sum += i;
        } else {
            int mid = start + (end - start) / 2;
            CountSumOfIntegers lt = new CountSumOfIntegers(start, mid);
            CountSumOfIntegers rt = new CountSumOfIntegers(mid + 1, end);
            lt.fork();
            rt.fork();
            int leftsum = lt.join();
            int rightsum = rt.join();
            sum = leftsum + rightsum;
        }
        return sum;
    }
}
