package Thread_;

import java.util.concurrent.CountDownLatch;

/**
 * @author zhaoyu
 * @date 2020/6/2
 * <p>
 * 闭锁，在完成某些运算时，只有其他所有线程的运算全部完成，当前运算才继续执行
 */
public class TestCOuntDownLatch {

    public static void main(String[] args) {

        final CountDownLatch cdl = new CountDownLatch(2);

        Cai cai = new Cai(cdl);
        Guo guo = new Guo(cdl);

        new Thread(cai).start();
        new Thread(guo).start();

        try {
            cdl.await();
            System.out.println("over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

class Cai implements Runnable {
    private CountDownLatch cdl;

    public Cai(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(6000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cdl.countDown();
        }

        System.out.println("买菜");
    }
}

class Guo implements Runnable {
    private CountDownLatch cdl;

    public Guo(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cdl.countDown();
        }

        System.out.println("买guo");
    }
}
