package current_;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zhaoyu
 * @date 2020/6/2
 * CopyOnWriteArrayList/CopyOnWriteArraySet “写入并复制”
 * 注意：添加操作多时，效率低， 因为每次添加时都会进行复制，开销非常的大，
 * 并发迭代时可以使用这个
 *
 */
public class TestCopyOnWriteArrayList {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(new MyThread()).start();
        }
    }
}

class MyThread implements Runnable {

    //private static ArrayList<String> list = new ArrayList();
    private static CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList();

    static {
        list.add("AA");
        list.add("BB");
        list.add("CC");
        list.add("DD");

    }

    @Override
    public void run() {
        for (String s : list) {
            System.out.println(s);
            list.add("AA");
        }
    }
}
