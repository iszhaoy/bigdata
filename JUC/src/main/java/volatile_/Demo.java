package volatile_;

/**
 * @author zhaoyu
 * @date 2020/6/1
 *
 * volatile : 不能保证互斥， 不能保证原子性 ，只是解决了内存可见性问题
 */
public class Demo {
    public static void main(String[] args) {
        FlagThread thread = new FlagThread();
        new Thread(thread).start();

        while (true) {
            if (thread.isFlag()) {
                System.out.println("main~~~~");
                break;
            }
        }
    }
}

class FlagThread implements Runnable {

    private volatile boolean flag;

    @Override
    public void run() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flag = true;
        System.out.println("flag " + flag);
    }


    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
