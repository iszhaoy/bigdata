package lock_;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhaoyu
 * @date 2020/6/4
 */
public class TestFairLock {
    public static void main(String[] args) {
        FairLock fl1 = new FairLock();
        //FairLock fl2 = new FairLock();
        UnFairLock fl3 = new UnFairLock();

        //new Thread(fl1,"A").start();
        //new Thread(fl1,"B").start();
        //new Thread(fl2,"B").start();
        new Thread(fl3,"A").start();
        new Thread(fl3,"B").start();
    }

}

class FairLock implements Runnable {

    // 公平锁
    //private static ReentrantLock lock = new ReentrantLock(true);
    private  ReentrantLock lock = new ReentrantLock(true);

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                System.out.println(Thread.currentThread().getName());
            } finally {
                lock.unlock();
            }
        }
    }

}


class UnFairLock implements Runnable {

    // 公平锁
    //private static ReentrantLock lock = new ReentrantLock(false);
    private  ReentrantLock lock = new ReentrantLock(false);

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                System.out.println(Thread.currentThread().getName());
            } finally {
                lock.unlock();
            }
        }
    }

}
