package com.bull.aurocontrol.csst.poc1;

import java.util.Date;

public interface FlightSeason {

    public abstract long indexSize();

    public abstract int getDuration();

    public abstract Date getLastDayOfSeason();

    public abstract Date getFirstDayOfSeason();

    public abstract int getNumberOfFlights();

}
