package weakRefrence_;

import java.lang.ref.WeakReference;

public class TestWeakReference01 {
    public static void main(String[] args) {
        // 在这样子 apple有一个强引用 是回收不了的
        //Apple apple = new Apple("好吃的苹果");
        //Salad salad = new Salad(apple);

        Salad salad = new Salad(new Apple("好吃的苹果"));

        //通过WeakReference的get()方法获取Apple
        System.out.println("Apple:" + salad.get());
        System.gc();

        try {
            // 休眠一下，在运行的时候加上虚拟机参数-XX:+PrintGCDetails，输出gc信息，确定gc发生了
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 如果为空，代表被回收了
        if (salad.get() == null) {
            System.out.println("clear apple");
        }
    }
}


class Salad extends WeakReference<Apple> {
    public Salad(Apple apple) {
        super(apple);
    }
}

class Apple {

    private String name;

    public Apple(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * 覆盖finalize，在回收的时候会执行。
     *
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        System.out.println("Apple： " + name + " finalize。");
    }

    @Override
    public String toString() {
        return "Apple{" +
                "name='" + name + '\'' +
                '}' + ", hashCode:" + this.hashCode();
    }
}

