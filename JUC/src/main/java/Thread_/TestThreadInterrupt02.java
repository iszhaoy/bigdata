package Thread_;

import java.io.IOException;

/**
 * @author zhaoyu
 * @date 2020/6/3
 */
public class TestThreadInterrupt02 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Thread thread = new Thread(new MyThread());
        thread.start();

        Thread.sleep(2000);
        System.out.println("start interrupted MyThread");
        thread.interrupt();
        System.out.println("Mythread is " + thread.isInterrupted());
    }
}

class MyThread implements Runnable {

    @Override
    public void run() {
        // 可以正常中断


        while (!Thread.interrupted()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            System.out.println("test ");
        }
    }

    //@Override
    //public void run() {
    //    // 不可以正常中断
    //    while (true) {
    //        System.out.println("test ");
    //    }
    //}
}