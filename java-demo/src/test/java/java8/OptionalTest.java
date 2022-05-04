package java8;


import com.iszhaoy.domain.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class OptionalTest {


    /**
     * 尝试访问 emptyOpt 变量的值会导致 NoSuchElementException。
     */
    @Test(expected = NoSuchElementException.class)
    public void whenCreateEmptyOptional_thenNull() {
        Optional<User> emptyOpt = Optional.empty();
        emptyOpt.get();
    }

    /**
     * 使用  of() 和 ofNullable() 方法创建包含值的 Optional。
     * 两个方法的不同之处在于如果你把 null 值作为参数传递进去，of() 方法会抛出 NullPointerException
     * 因此，你应该明确对象不为 null  的时候使用 of()。
     * 如果对象即可能是 null 也可能是非 null，你就应该使用 ofNullable() 方法
     */
    @Test(expected = NullPointerException.class)
    public void whenCreateOfEmptyOptional_thenNullPointerException() {
        User user = null;

        Optional<User> opt = Optional.of(user);
        //Optional<User> opt = Optional.ofNullable(user);
    }

    /**
     * 从 Optional 实例中取回实际值对象的方法之一是使用 get() 方法
     */
    @Test
    public void whenCreateOfNullableOptional_thenOk() {
        String name = "John";
        Optional<String> opt = Optional.ofNullable(name);

        assertEquals("John", opt.get());
    }

    /**
     * 要避免异常，你可以选择首先验证是否有值
     */
    @Test
    public void whenCheckIfPresent_thenOk() {
        User user = new User();
        user.setUserId("1001");
        Optional<User> opt = Optional.ofNullable(user);
        //Optional<User> opt = Optional.ofNullable(null);

        //assertTrue(opt.isPresent());

        //检查是否有值的另一个选择是 ifPresent() 方法。该方法除了执行检查，还接受一个Consumer(消费者) 参数，如果对象不是空的，就对执行传入的 Lambda 表达式：
        opt.ifPresent(u -> assertEquals(u.getUserId(),u.getUserId()));

        assertEquals(user.getUserId(), opt.get().getUserId());
    }

    /**
     * optional 类提供了 API 用以返回对象值，或者在对象为空的时候返回默认值。
     *
     * 这里你可以使用的第一个方法是 orElse()，它的工作方式非常直接，如果有值则返回该值，否则返回传递给它的参数值
     */
    @Test
    public void whenEmptyValue_thenReturnDefault() {
        User user = null;
        User user2 = new User("anna@gmail.com", "1234");
        //这里 user 对象是空的，所以返回了作为默认值的 user2。
        User result = Optional.ofNullable(user).orElse(user2);

        assertEquals(user2.getEmail(), result.getEmail());
    }

    @Test
    public void whenValueNotNull_thenIgnoreDefault() {
        User user = new User("john@gmail.com","1234");
        User user2 = new User("anna@gmail.com", "1234");
        User result = Optional.ofNullable(user).orElse(user2);
        // 如果对象的初始值不是 null，那么默认值会被忽略：
        assertEquals("john@gmail.com", result.getEmail());
    }
    // 第二个同类型的 API 是 orElseGet() —— 其行为略有不同。这个方法会在有值的时候返回值，
    // 如果没有值，它会执行作为参数传入的 Supplier(供应者) 函数式接口，并将返回其执行结果：
    // User result = Optional.ofNullable(user).orElseGet( () -> user2);

    @Test
    public void givenEmptyValue_whenCompare_thenOk() {
        //User user = null;
        User user = new User();
        System.out.println("Using orElse");
        User result = Optional.ofNullable(user).orElse(createNewUser());
        System.out.println(result);
        System.out.println("Using orElseGet");
        User result2 = Optional.ofNullable(user).orElseGet(() -> createNewUser());
    }

    private User createNewUser() {
        System.out.println("Creating New User");
        return new User("extra@gmail.com", "1234");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenThrowException_thenOk() {
        User user = new User("anna@gmail.com", "1234");
        User result = Optional.ofNullable(user)
                .orElseThrow( () -> new IllegalArgumentException());
    }

    // 转换值
    //有很多种方法可以转换 Optional  的值。我们从 map() 和 flatMap() 方法开始。
    //先来看一个使用 map() API 的例子：
    @Test
    public void whenMap_thenOk() {
        User user = new User("anna@gmail.com", "1234");
        String email = Optional.ofNullable(user)
                .map(u -> u.getEmail()).orElse("default@gmail.com");

        assertEquals(email, user.getEmail());
    }

    /*
    * map() 对值应用(调用)作为参数的函数，然后将返回的值包装在 Optional 中。这就使对返回值进行链试调用的操作成为可能 —— 这里的下一环就是 orElse()。
    相比这下，flatMap() 也需要函数作为参数，并对值调用这个函数，然后直接返回结果。
    下面的操作中，我们给 User 类添加了一个方法，用来返回 Optional：
    * */

    @Test
    public void whenFlatMap_thenOk() {
        User user = new User("anna@gmail.com", "1234");
        //user.setPosition("Developer");
        user.setPosition(null);
        String position = Optional.ofNullable(user)
                .flatMap(u -> u.getPosition())
                .orElse("default");
        assertEquals(position, "default");
    }

    /*
    *   过滤值
        除了转换值之外，Optional  类也提供了按条件“过滤”值的方法。
        filter() 接受一个 Predicate 参数，返回测试结果为 true 的值。如果测试结果为 false，会返回一个空的 Optional。
        来看一个根据基本的电子邮箱验证来决定接受或拒绝 User(用户) 的示例：
    * */
    @Test
    public void whenFilter_thenOk() {
        User user = new User("anna@gmail.com", "1234");
        Optional<User> result = Optional.ofNullable(user)
                .filter(u -> u.getEmail() != null && u.getEmail().contains("@"));

        assertTrue(result.isPresent());
    }
}

