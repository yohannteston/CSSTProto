package com.bull.aurocontrol.csst.poc;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public interface FlightSeasonFactory {

   
    FlightSeason buildFlightSeason(Iterator<Flight> source, int bufferDuration);

}