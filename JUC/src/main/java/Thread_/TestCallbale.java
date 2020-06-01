package Thread_;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Callable 可以有返回值，并且可以抛出异常
 */
public class TestCallbale {
    public static void main(String[] args) {
        ThreadDemo td = new ThreadDemo();

        // 执行Callable方式，需要FutureTask实现类的试吃，用于接收运算结果，FutureTask 是Future接口的实现类

        FutureTask<Integer> result = new FutureTask<>(td);

        new Thread(result).start();
        System.out.println("~~~~~~~~~~~~~~~``");
        try {
            Integer sum = result.get();
            System.out.println(sum);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}


class ThreadDemo implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        int sum = 0;
        for (int i = 0; i < 10000; i++) {
            sum++;
        }
        return sum;
    }
}