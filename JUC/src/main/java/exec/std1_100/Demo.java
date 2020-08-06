package exec.std1_100;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/29 17:09
 */
public class Demo {
    private static final Object lock = new Object();
    private volatile boolean aHasprint = false;

    class A implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i <= 100; i += 2) {
                synchronized (lock) {
                    if (aHasprint) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("A:" + i);
                    aHasprint = true;
                    lock.notify();
                }

            }
        }
    }

    class B implements Runnable {
        @Override
        public void run() {
            for (int i = 1; i <= 100; i += 2) {
                synchronized (lock) {
                    if (!aHasprint) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("B:" + i);
                    aHasprint = false;
                    lock.notify();
                }
            }
        }
    }

    public static void main(String[] args) {
        Demo solution2 = new Demo();
        Thread threadA = new Thread(solution2.new A());
        Thread threadB = new Thread(solution2.new B());
        threadA.start();
        threadB.start();
    }
}
