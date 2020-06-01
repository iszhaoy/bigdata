package join;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/6/4 16:34
 */
public class JoinDemo {

    public static void main(String[] args) {
        Thread scanDouyinThread = new Thread(new ScanDouYin());
        Thread sleep = new Thread(new ReadySleep(scanDouyinThread));
        sleep.start();
        scanDouyinThread.start();
    }
}

class ScanDouYin implements Runnable {
    // ms
    private int time = 10000;

    @Override
    public void run() {
        System.out.println("开始刷抖音喽~~~");
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("刷美了~~~");
    }
}

class ReadySleep implements Runnable {

    private Thread scanDouYin;

    public ReadySleep(Thread scanDouYin) {
        this.scanDouYin = scanDouYin;
    }

    @Override
    public void run() {
        try {
            System.out.println("准备睡觉了。。。。");
            scanDouYin.join();
            System.out.println("睡觉吧。。。。");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
