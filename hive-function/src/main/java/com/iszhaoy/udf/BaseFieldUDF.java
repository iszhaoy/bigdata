package com.iszhaoy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;


public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String key) {

        // 切割
        String[] log = line.split("\\|");

        // 合法性判断
        if (log.length != 2 || StringUtils.isBlank(log[1].trim())) {
            return "";
        }

        JSONObject json = new JSONObject(log[1].trim());

        String result = "";

        // 根据传进来的key取值
        if ("st".equals(key)) {

            // 返回服务器时间
            result = log[0].trim();
        } else if ("et".equals(key)) {

            if (json.has("et")) {
                result = json.getString("et");
            }

        } else {
            // 获取cm对应的value值
            JSONObject cm = json.getJSONObject("cm");
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }

        return result;
    }

    public static void main(String[] args) {
        String str = "1592927892823|{\"cm\":{\"ln\":\"-54.0\",\"sv\":\"V2.7.7\",\"os\":\"8.2.2\",\"g\":\"FJG3VA20@gmail.com\",\"mid\":\"1\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"1\",\"t\":\"1592838922578\",\"la\":\"-12.8\",\"md\":\"HTC-0\",\"vn\":\"1.0.3\",\"ba\":\"HTC\",\"sr\":\"N\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1592878862545\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"1\",\"category\":\"7\"}},{\"ett\":\"1592874539582\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"1\",\"goodsid\":\"1\",\"news_staytime\":\"9\",\"loading_time\":\"0\",\"action\":\"3\",\"showtype\":\"3\",\"category\":\"31\",\"type1\":\"\"}},{\"ett\":\"1592908720209\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1592926447649\",\"action\":\"4\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1592888354082\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\\\n at java.lang.reflect.Method.invoke(Method.java:606)\\\\n\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\"}}]}";
        System.out.println(new BaseFieldUDF().evaluate(str, "mid"));
        System.out.println(new BaseFieldUDF().evaluate(str, "st"));
        System.out.println(new BaseFieldUDF().evaluate(str, "et"));
    }
}
