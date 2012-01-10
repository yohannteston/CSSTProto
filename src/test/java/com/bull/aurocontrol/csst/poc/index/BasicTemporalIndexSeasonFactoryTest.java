package com.bull.aurocontrol.csst.poc.index;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bull.aurocontrol.csst.poc1.DayOfWeek;
import com.bull.aurocontrol.csst.poc1.Flight;
import com.bull.aurocontrol.csst.poc1.FlightSeason;
import com.bull.eurocontrol.csst.poc.source.CSVFlightSourceFactory;
import com.bull.eurocontrol.csst.poc.utils.MemoryWatch;

public class BasicTemporalIndexSeasonFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {

        
        
        BasicTemporalIndexSeasonFactory factory = new BasicTemporalIndexSeasonFactory();
        
        Flight flight = new Flight();
        Date fdow = DateUtils.truncate(new Date(), Calendar.DATE);
        Date ldow = DateUtils.addDays(fdow, 10);
        flight.setFirstDayOfOperation(fdow);
        flight.setLastDayOfOperation(ldow);
        flight.setDepartureAirport("DEP");
        flight.setEobt(30);
        flight.setDaysOfOperation(EnumSet.allOf(DayOfWeek.class));
        
        
        Iterator<Flight> source = Collections.singleton(flight).iterator();
        
        FlightSeason season =  factory.buildFlightSeason(source, 30);
        System.out.println(season);
    }
    
    @Test
    public void testRealSource() {
        MemoryWatch memoryWatch = new MemoryWatch();
        memoryWatch.start();
        
        CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(new File("5AO.csv"), new File("PROFILE_WITHOUT_CIRCULAR.csv"));

        
        
        BasicTemporalIndexSeasonFactory factory = new BasicTemporalIndexSeasonFactory();
        
      
        Iterator<Flight> source = sourceFactory.iterate();
        
        FlightSeason season =  factory.buildFlightSeason(source, 30);
        memoryWatch.stop();
        System.out.println(memoryWatch);
        System.out.println(season);
        
    }
    

}
