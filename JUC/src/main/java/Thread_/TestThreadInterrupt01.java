package Thread_;

import java.io.IOException;

/**
 * @author zhaoyu
 * @date 2020/6/3
 */
public class TestThreadInterrupt01 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Thread thread = new ThreadInterrupt();
        thread.start();
        System.out.println("在50秒之内按任意键中断线程!");
        System.in.read();
        thread.interrupt();
        thread.join();
        System.out.println("线程已经退出!");
    }
}

class ThreadInterrupt extends Thread {
    public void run() {
        try {
            sleep(50000);  // 延迟50秒
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}