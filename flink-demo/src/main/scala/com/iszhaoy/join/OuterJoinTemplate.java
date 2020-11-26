package com.iszhaoy.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class OuterJoinTemplate implements CoGroupFunction<BaseClass, BaseClass, BaseClass> {

    // 左流join key
    private String leftKey;

    // 右流join key
    private String rightKey;

    // other fileds
    private String leftFileds;

    private String rightFileds;

    private OuterJoinTemplate() {

    }

    public OuterJoinTemplate(String leftKey, String rightKey,String leftFileds,String rightFileds) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.leftFileds = leftFileds;
        this.rightFileds = rightFileds;
    }

    @Override
    public void coGroup(Iterable<BaseClass> first, Iterable<BaseClass> second, Collector<BaseClass> out) throws Exception {

        String[] leftFields = this.leftFileds.split(",");
        String[] rightFields = this.rightFileds.split(",");

        for (BaseClass left : first) {
            for (String leftField : leftFields) {

            }
            boolean isMatched = false;
            for (BaseClass right : second) {
                Object leftKey = left.getValue("key");

            }
        }
    }
}


@SuppressWarnings("unchecked")
@Log4j
abstract class BaseClass<T> {

    private final BaseClass subClass = setSubClass();

    private BaseClass setSubClass() {
        return this;
    }

    public abstract void setKey(T key);

    public T getValue(String param) {

        if (param == null) throw new NullPointerException();

        Class clz = this.subClass.getClass();
        T value = null;
        boolean isFind = false;
        do {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                if (param.equals(field.getName())) {
                    try {
                        Method getKeyMethod = clz.getDeclaredMethod(field.getName());
                        value = (T) getKeyMethod.invoke(this.subClass);
                        isFind = true;
                    } catch (NoSuchMethodException e) {
                        log.error("获取value失败");
                        e.printStackTrace();
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            clz = clz.getSuperclass();
        } while (clz != Object.class && !isFind);
        return value;
    }

}

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
class Left extends BaseClass<String> {
    private String key;
    private String value;
    private Long dateTime;


    @Override
    public void setKey(String key) {

    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
class Right extends BaseClass<String> {
    private String key;
    private String value;
    private Long dateTime;

    @Override
    public void setKey(String key) {

    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
class Result extends BaseClass<String> {
    private String key;
    private String leftValues;
    private String rightValues;
    private Long dateTime;

    @Override
    public void setKey(String key) {

    }
}