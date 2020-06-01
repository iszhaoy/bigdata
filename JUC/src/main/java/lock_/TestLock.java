package lock_;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhaoyu
 * @date 2020/6/3
 */
public class TestLock {
    public static void main(String[] args) throws InterruptedException {
        Ticket ticket = new Ticket(10);

        Lock lock = new ReentrantLock();

        Thread t1 = new Thread(new Sell(ticket, lock));
        Thread t2 = new Thread(new Sell(ticket, lock));
        Thread t3 = new Thread(new Sell(ticket, lock));
        Thread t4 = new Thread(new Sell(ticket, lock));
        t1.start();
        t2.start();
        //t3.start();
        //t4.start();

        //try {
        //    Thread.sleep(2000);
        //} catch (InterruptedException e) {
        //    e.printStackTrace();
        //}

        while (true) {
            Thread.sleep(1000);
            System.out.println("main" + ticket.getNum());
            if (ticket.getNum() == 0) {
                System.out.println("主线程通知线程1中断");
                t1.interrupt();
                System.out.println("主线程通知线程2中断");
                t2.interrupt();
                //t3.interrupt();
                //t4.interrupt();
                break;

            }
        }


    }
}


class Ticket {
    private int num;

    public Ticket(int num) {
        this.num = num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

class Sell implements Runnable {

    private Ticket ticket;

    private Lock lock;

    public Sell(Ticket ticket, Lock lock) {
        this.ticket = ticket;
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
                try {

                    lock.lock();

                    if (ticket.getNum() > 0) {
                        ticket.setNum(ticket.getNum() - 1);
                        System.out.println(Thread.currentThread().getId() + ",sell 1 ticket, 剩余" + ticket.getNum());

                    }

                } finally {

                    lock.unlock();
                }
            }

        } catch (InterruptedException e) {
            System.out.println("interrupt");
        } finally {
            System.out.println("票卖完了，中断");
        }
    }
}
