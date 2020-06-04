package status_;

/**
 * @author zhaoyu
 * @date 2020/6/4
 */
public class TestThreadStatus {
    public static void main(String[] args) throws InterruptedException {
        Thread test_run = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("test run");
            }
        });
        System.out.println(test_run.getState());

        System.out.println("````````````");

        Thread test_runnable = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("test runnable");
            }
        });
        test_runnable.start();
        System.out.println(test_runnable.getState());

        //System.out.println("````````````");

        //Thread block1 = new Thread(new BlockThread());
        //Thread block2 = new Thread(new BlockThread());
        //block1.start();
        //Thread.sleep(2000);
        //block2.start();
        //while(true) {
        //    System.out.println(block2.getState());
        //}

        System.out.println("````````````");
        Thread test_waite = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("test waite");
            }
        });

        test_waite.start();
        test_waite.join();
        System.out.println(test_waite.getState());
    }
}

class BlockThread implements Runnable {

    private int num;

    @Override
    public synchronized void run() {
        while (true) {
            System.out.println(num);
        }
    }
}
