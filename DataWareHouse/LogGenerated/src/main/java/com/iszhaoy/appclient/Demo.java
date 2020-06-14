package com.iszhaoy.appclient;

/**
 * @author zhaoyu
 * @date 2020/6/13
 */
public class Demo {

    public static void main(String[] args) {

    }

    public  static boolean  fn1(String s1,String s2) {
        if (s1 == null || s2 == null) {
            return false;
        }
        if (s1.length() != s2.length()) return false;

        char[] chars1 = s1.toCharArray();
        char[] chars2 = s2.toCharArray();

        for (int i = 0; i < chars1.length; i++) {
            if (chars1[i] != chars2[i]) {
                return false;
            }
        }
        return true;
    }
}
