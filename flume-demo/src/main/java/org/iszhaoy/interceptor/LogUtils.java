package org.iszhaoy.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {

    /**
     * @param log
     * @return
     * 验证启动日志
     */
    public static boolean validateStart(String log) {

        if (log == null) {
            return false;
        }

        // 判断数据是否是大括号开头和大括号结尾
        return log.trim().startsWith("{") && log.trim().endsWith("}");
    }

    /**
     * @param log
     * @return
     * 验证事件日志
     */
    public static boolean validateEvent(String log) {

        // 服务器时间|日志内容
        if (log == null) {
            return false;
        }

        // 切割
        String[] logContents = log.split("\\|");
        if (logContents.length != 2) {
            return false;
        }

        // 校验服务器时间（长度必须是13位,必须都是数字）
        if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])) {
            return false;
        }

        // 校验日志内容
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")) {
            return false;
        }

        return true;
    }
}
