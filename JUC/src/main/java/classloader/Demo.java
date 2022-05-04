package classloader;

import java.io.IOException;
import java.io.InputStream;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/8/28 15:40
 */
public class Demo {
    public static void main(String[] args) throws Exception {

        ClassLoader myClassLoader = new ClassLoader() {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                try {
                    String fileName = name.substring(name.lastIndexOf(".") + 1) + ".class";
                    InputStream inputStream = getClass().getResourceAsStream(fileName);
                    if (inputStream == null) {
                        return super.findClass(name);
                    }
                    byte[] bytes = new byte[inputStream.available()];
                    inputStream.read(bytes);
                    return defineClass(name, bytes, 0, bytes.length);
                } catch (IOException e) {
                    throw new ClassNotFoundException();
                }
            }

        };
        // 不同的类加载器加载的类在不同的命名空间，所以这里是false
        Object cl = myClassLoader.loadClass("classloader.Demo");
        System.out.println(cl instanceof Demo);
    }
}
