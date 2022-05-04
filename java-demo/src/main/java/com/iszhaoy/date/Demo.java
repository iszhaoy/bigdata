package com.iszhaoy.date;

import java.util.Calendar;
import java.util.Date;

public class Demo {
    public static void main(String[] args) {

        long timeMillis = System.currentTimeMillis();
        //System.out.println(new Date(timeMillis));

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timeMillis);

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        //System.out.println(year + "-" + month);

        calendar.set(Calendar.YEAR, year + 1);
        calendar.set(Calendar.MONTH,month + 1);


        calendar.set(Calendar.DAY_OF_MONTH,32);
        System.out.println(new Date(calendar.getTimeInMillis()));
        calendar.set(Calendar.DAY_OF_MONTH,31 + 1);
        System.out.println(new Date(calendar.getTimeInMillis()));
    }
}
