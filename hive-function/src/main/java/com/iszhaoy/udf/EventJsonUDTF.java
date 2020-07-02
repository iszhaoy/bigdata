package com.iszhaoy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 定义UDTF返回值类型和名称
        // 定义UDTF返回值类型和名称
        List<String> fieldName = new ArrayList<>();
        List<ObjectInspector> fieldType = new ArrayList<>();


        fieldName.add("event_name");
        fieldName.add("event_json");

        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 传入的是 json array => UDF 传入et
        String input = objects[0].toString();

        if (StringUtils.isBlank(input)) {
            return;
        } else {
            JSONArray jsonArray = new JSONArray(input);

            if (jsonArray == null) {
                return;
            }

            // 循环遍历Array当中每一个元素，封装成返回的， 时间名称和时间内容
            for (int i = 0; i < jsonArray.length(); i++) {

                String[] result = new String[2];
                try {
                    // 取出每个事件的类型
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    // 去重每个事件的json
                    result[1] = jsonArray.getString(i);
                } catch (JSONException e) {
                    continue;
                }
                // 写出
                forward(result);
            }

        }
    }

    @Override
    public void close() throws HiveException {

    }
}
