package com.bull.aurocontrol.csst.poc;

import java.util.Date;
import java.util.EnumSet;

public class FlightSchedule implements Comparable<FlightSchedule> {
    private EnumSet<DayOfWeek> daysOfOperation;
    private long firstDayOfOperation;
    private long lastDayOfOperation;

    public FlightSchedule() {
    }

    public long getFirstDayOfOperation() {
        return firstDayOfOperation;
    }

    public void setFirstDayOfOperation(long firstDayOfOperation) {
        this.firstDayOfOperation = firstDayOfOperation;
    }

    public long getLastDayOfOperation() {
        return lastDayOfOperation;
    }

    public void setLastDayOfOperation(long lastDayOfOperation) {
        this.lastDayOfOperation = lastDayOfOperation;
    }

    public EnumSet<DayOfWeek> getDaysOfOperation() {
        return daysOfOperation;
    }

    public void setDaysOfOperation(EnumSet<DayOfWeek> daysOfOperation) {
        this.daysOfOperation = daysOfOperation;
    }


    @Override
    public int compareTo(FlightSchedule other) {
        if (this == other) return 0;
        if (other == null) return -1;
        int r = (int) (firstDayOfOperation - other.firstDayOfOperation);
        if (r != 0) return r;
        r = (int) (lastDayOfOperation - other.lastDayOfOperation);
        return r;
    }

   

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((daysOfOperation == null) ? 0 : daysOfOperation.hashCode());
        result = prime * result + (int) (firstDayOfOperation ^ (firstDayOfOperation >>> 32));
        result = prime * result + (int) (lastDayOfOperation ^ (lastDayOfOperation >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        FlightSchedule other = (FlightSchedule) obj;
        if (daysOfOperation == null) {
            if (other.daysOfOperation != null) return false;
        } else if (!daysOfOperation.equals(other.daysOfOperation)) return false;
        if (firstDayOfOperation != other.firstDayOfOperation) return false;
        if (lastDayOfOperation != other.lastDayOfOperation) return false;
        return true;
    }

    @Override
    public String toString() {
        return "FlightSchedule [daysOfOperation=" + daysOfOperation + ", firstDayOfOperation=" + firstDayOfOperation + ", lastDayOfOperation="
                + lastDayOfOperation + "]";
    }
    
    
    
}