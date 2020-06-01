package join;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/6/4 16:30
 * <p>
 * 调用者调用被调用者的join后，会等待被调用者执行完然后在执行
 */
public class TestJoin {
    public static void main(String[] args) throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("执行t线程");
            }
        });

        Thread.yield();
        System.out.println("main 线程 调用 t线程");
        t.start();
        t.join();
        System.out.println("main 线程结束");

    }
}
