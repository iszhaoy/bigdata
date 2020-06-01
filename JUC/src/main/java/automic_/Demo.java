package automic_;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhaoyu
 * @date 2020/6/1
 *
 * 1> i++ 的原子性问题： i++ 的操作实际上分为三个步骤 “读 改 写”
 *    int i = 10;
 *    i = i++; // 10
 *
 *    int temp = i;
 *    i = i + 1
 *    i = temp;
 *
 * 2> 原子变量,jdk1.5后 java.util.concurrent.atomic 包下提供了常用的原子变量
 *           1. volatile 禁止指令重排序，保证内存可见性
 *           2. CAS算法保证数据的原子性 （compare and swap） 算法保证了数据的原子性
 *              CAS算法是硬件对于并发操作共享数据的支持
 *              CAS 包含了三个操作数：
 *                  内存值 v
 *                  预估值 A
 *                  更新值 B
 *                  当前晋档V == A ,V = B,否则将不做任何操作
 */
public class Demo {
    public static void main(String[] args) {
        AutomicThread at = new AutomicThread();
        for (int i = 0; i < 10; i++) {
            new Thread(at).start();
        }
    }
}

class AutomicThread implements  Runnable {

    //private int i;
    private AtomicInteger i = new AtomicInteger(0);

    @Override
    public void run() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(getI());
    }

    public int getI() {
        return i.getAndIncrement();
    }
}
