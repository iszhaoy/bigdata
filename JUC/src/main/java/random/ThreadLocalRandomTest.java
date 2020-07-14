package random;

import java.util.concurrent.ThreadLocalRandom;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/10 15:49
 */
public class ThreadLocalRandomTest {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(new ThreadRandom()).start();
        }
    }
}

class ThreadRandom implements Runnable {

    @Override
    public void run() {
        System.out.println("random num is : " + ThreadLocalRandom.current().nextInt(10));
    }
}
