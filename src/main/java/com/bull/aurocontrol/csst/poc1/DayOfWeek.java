package com.bull.aurocontrol.csst.poc1;

import java.util.Calendar;
import java.util.Date;

public enum DayOfWeek {
    MONDAY,
    TUESDAY,
    WEDNESDAY,
    THIRSDAY,
    FRIDAY,
    SATURDAY,
    SUNDAY;
    
    
    public DayOfWeek next() {
        return values()[(this.ordinal() + 1) % 7]; 
    }

    public static DayOfWeek get(Date firstDayOfOperation) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(firstDayOfOperation);
        return DayOfWeek.values()[cal.get(Calendar.DAY_OF_WEEK)-1];
    }
    public static DayOfWeek get(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        return DayOfWeek.values()[cal.get(Calendar.DAY_OF_WEEK)-1];
    }
}
