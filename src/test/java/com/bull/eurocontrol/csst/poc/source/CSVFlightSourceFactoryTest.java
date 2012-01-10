package com.bull.eurocontrol.csst.poc.source;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bull.aurocontrol.csst.poc1.Flight;

public class CSVFlightSourceFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testIterate() {
        CSVFlightSourceFactory factory = new CSVFlightSourceFactory(new File("CSS_SCHEDULE-FINS11.csv"), new File("PROFILE_WITHOUT_CIRCULAR.csv"));
        Iterator<Flight> iterator = factory.iterate();
        while (iterator.hasNext()) {
            Flight flight = (Flight) iterator.next();
            System.out.println(flight);
        }
    }

}
