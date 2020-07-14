package jvm;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/10 9:53
 *
 * top -Hp pid
 * print "%x\n" -> tid
 * jstack pid | grep tid
 */
public class DeadLock {
    public static void main(String[] args) {
        new Thread(new AThread()).start();
        new Thread(new BThread()).start();
    }
}

class A {
    public static void print() {
        System.out.println("A");
    }
}

class B {
    public static void print() {
        System.out.println("B");
    }
}

class AThread implements Runnable {

    @Override
    public void run() {
        while (true) {
            synchronized (A.class) {
                A.print();
               synchronized (B.class) {
                   B.print();
               }
            }
        }
    }
}

class BThread implements Runnable {

    @Override
    public void run() {
        while (true) {
            synchronized (B.class) {
                B.print();
               synchronized(A.class) {
                   A.print();
               }
            }
        }
    }
}
