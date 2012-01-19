package com.bull.aurocontrol.csst.poc;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

public class ConflictQuery {

    private String adep;
    private String ades;
    private Date from;
    private Date to;
    private int minimumConflicts;

    public ConflictQuery(String adep, String ades, Date from, Date to, int minimumConflicts) {
        super();
        this.adep = adep;
        this.ades = ades;
        this.from = (from == null) ? null : DateUtils.truncate(from, Calendar.DAY_OF_MONTH);
        this.to = (to == null) ? null : DateUtils.truncate(to, Calendar.DAY_OF_MONTH);
        this.minimumConflicts = Math.max(minimumConflicts, 0);
    }

    public String getAdep() {
        return adep;
    }

    public String getAdes() {
        return ades;
    }

    public Date getFrom() {
        return from;
    }

    public Date getTo() {
        return to;
    }

    public int getMinimumConflicts() {
        return minimumConflicts;
    }

    @Override
    public String toString() {
        String from = (this.from == null) ? null :FastDateFormat.getInstance("yyyyMMdd").format(this.from);
        String to = (this.to == null) ? null :FastDateFormat.getInstance("yyyyMMdd").format(this.to);
        return "ConflictQuery [adep=" + adep + ", ades=" + ades + ", from=" +  from + ", to=" + to + ", minimumConflicts=" + minimumConflicts + "]";
    }

}
