package automic_;

/**
 * @author zhaoyu
 * @date 2020/6/1
 */
public class CASDemo {

    public static void main(String[] args) {
        CompareAndSwap cas = new CompareAndSwap();

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                int expectedValue = cas.get();
                boolean b = cas.compareAndSet(expectedValue, (int) (Math.random() * 101));
                System.out.println(b);
            }).start();
        }
    }
}

class CompareAndSwap {

    private int value;

    //  获取内存值
    public synchronized int get() {
        return value;
    }

    // 比较
    public synchronized int compareAndSwap(int expecteValue, int newValue) {
        int oldValue = value;
        if (oldValue == expecteValue) {
            this.value = newValue;
        }
        return oldValue;
    }

    public synchronized boolean compareAndSet(int expectedValue, int newValue) {
        return expectedValue == compareAndSwap(expectedValue, newValue);
    }


}