package java8;

import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class FunctionTest {

    @Test
    public void test01() {
        Function<Integer, Integer> f1 = (i) -> i + 1;
        Function<Integer, Integer> f2 = (i) -> i * i;


        assertEquals(Integer.valueOf(6), f1.apply(5));
        assertEquals(Integer.valueOf(25), f2.apply(5));

    }

    @Test
    public void test02() {
        Function<Integer, Integer> f1 = (i) -> i + 1;
        Function<Integer, Integer> f2 = (i) -> i * i;

        assertEquals(Integer.valueOf(36),f2.compose(f1).apply(5));
        assertEquals(Integer.valueOf(26),f2.andThen(f1).apply(5));

    }

    @FunctionalInterface
    interface Function3 <A, B, C, R> {
        //R is like Return, but doesn't have to be last in the list nor named R.
        public R apply (A a, B b, C c);
    }

    @Test
    public void test03() {
        Function3<String, Integer, Double, String> f = (a, b, c) -> {
            StringBuilder sb = new StringBuilder();
            return sb.append(a).append(b).append(c).toString();
        };

        assertEquals("test12.9",f.apply("test", 1, 2.9));
    }


    @Test
    public void test04() {
        Function<String, String> funciton1 = (s) -> s + "吗";
        Function<String, String> funciton2 = (s) -> "你" + s;
        Function<String, String> funciton3 = (s) -> s + "?";
        System.out.println(funciton3.compose(funciton2.andThen(funciton1)).apply("配"));
    }
}
