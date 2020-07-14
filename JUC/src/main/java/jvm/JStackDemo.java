package jvm;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/9 16:14
 */
public class JStackDemo {
    public static void main(String[] args) {
        new Thread(new TestJStackThread()).start();
    }
}
class TestJStackThread implements Runnable {

    @Override
    public void run() {
        while (true) {
            System.out.println(1);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}