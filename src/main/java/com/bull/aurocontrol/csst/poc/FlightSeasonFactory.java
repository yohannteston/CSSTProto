package com.bull.aurocontrol.csst.poc;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import jsr166y.ForkJoinPool;

public interface FlightSeasonFactory {

   
    
    FlightSeason buildFlightSeason(Iterator<Flight> source, int bufferDuration, ForkJoinPool pool);

}