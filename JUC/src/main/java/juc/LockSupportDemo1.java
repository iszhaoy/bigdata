package juc;

import java.util.concurrent.locks.LockSupport;

public class LockSupportDemo1 {
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("thread start");
                LockSupport.park();
                System.out.println("thread end");
            }
        });
        thread.start();
        // 睡眠
        Thread.sleep(1000);
        //执行unpark
        LockSupport.unpark(thread);
    }
}
