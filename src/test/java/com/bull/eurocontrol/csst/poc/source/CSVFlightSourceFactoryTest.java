package com.bull.eurocontrol.csst.poc.source;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bull.aurocontrol.csst.poc.Flight;

public class CSVFlightSourceFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    
    public void testIterate() {
        CSVFlightSourceFactory factory = new CSVFlightSourceFactory(new File("5AO.csv"), new File("PROFILE_WITHOUT_CIRCULAR.csv"));
        Iterator<Flight> iterator = factory.iterate();
        while (iterator.hasNext()) {
            Flight flight = (Flight) iterator.next();
            System.out.println(flight);
        }
    }

}
