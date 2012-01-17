package com.bull.aurocontrol.csst.poc;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.mutable.MutableInt;

import com.bull.eurocontrol.csst.poc.utils.SMatrix;


public interface FlightSeason {

    public abstract long indexSize();

    public abstract int getDuration();

    public abstract Date getLastDayOfSeason();

    public abstract Date getFirstDayOfSeason();

    public abstract int getNumberOfFlights();


    SMatrix<Integer> queryOverlaps(int paralellism) throws IOException;

}
