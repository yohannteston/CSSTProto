package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;

import com.bull.aurocontrol.csst.poc1.DayOfWeek;
import com.bull.aurocontrol.csst.poc1.Flight;
import com.bull.aurocontrol.csst.poc1.FlightSeason;
import com.bull.aurocontrol.csst.poc1.FlightSeasonFactory;

public class BasicTemporalIndexSeasonFactory implements FlightSeasonFactory {

    private static final int MINUTE_PER_DAY = 60*24;

    @Override
    public FlightSeason buildFlightSeason(Iterator<Flight> source, int bufferDuration) {
        StopWatch loading = new StopWatch();
        loading.start();
        
        long firstDayOfSeason = Long.MAX_VALUE;            
        long lastDayOfSeason = Long.MIN_VALUE;            
        
        List<Flight> db = new ArrayList<Flight>();
        while (source.hasNext()) {
            Flight flight = (Flight) source.next();
            db.add(flight);
            
            if (firstDayOfSeason > flight.getFirstDayOfOperation().getTime()) {
                firstDayOfSeason = flight.getFirstDayOfOperation().getTime();
            }
            if (lastDayOfSeason < flight.getLastDayOfOperation().getTime()) {
                lastDayOfSeason = flight.getLastDayOfOperation().getTime();
            }
             
        }            
        loading.stop();       

        HashMap<String, LocationOccupation> locations = new HashMap<String, LocationOccupation>();
        
        for (int i = 0, endi = db.size(); i < endi; i++) {
            Flight flight = db.get(i);
            
            LocationOccupation adep = getOrCreateLocation(locations, flight.getDepartureAirport());
            LocationOccupation ades = getOrCreateLocation(locations, flight.getDestinationAirport());
            
            
            int firstDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(flight.getFirstDayOfOperation().getTime() - firstDayOfSeason);
            int lastDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(flight.getLastDayOfOperation().getTime() - firstDayOfSeason);
            
            
            DayOfWeek dow = DayOfWeek.get(flight.getFirstDayOfOperation());
            
            EnumSet<DayOfWeek> dowOfOperation = flight.getDaysOfOperation();
            
            for (int dayOfSeason = firstDayOfOpInSeason; dayOfSeason <= lastDayOfOpInSeason; dayOfSeason++, dow = dow.next()) {
                if (dowOfOperation.contains(dow)) {
                    int start = (dayOfSeason * MINUTE_PER_DAY + flight.getEobt() - bufferDuration) ;
                    int end = (dayOfSeason * MINUTE_PER_DAY + flight.getEobt() + bufferDuration) ;

                    adep.add(i, start, end);

                    start = (dayOfSeason * MINUTE_PER_DAY + flight.getEta() - bufferDuration) ;
                    end = (dayOfSeason * MINUTE_PER_DAY + flight.getEta() + bufferDuration) ;

                    ades.add(i, start, end);
                    
                    for (int j = 0, endj = flight.countAirspace(); j < endj; j++) {
                        start = (dayOfSeason * MINUTE_PER_DAY + flight.getStart(j) - bufferDuration) ;
                        end = (dayOfSeason * MINUTE_PER_DAY + flight.getEnd(j) + bufferDuration) ;
                        getOrCreateLocation(locations, flight.getAirspace(j));
                    }
                }                
            }

            
            
        }
        for (LocationOccupation occ : locations.values()) {
           // occ.optimize();
        }
        
        return new FlightSeasonImpl(locations);
    }

    private LocationOccupation getOrCreateLocation(HashMap<String, LocationOccupation> locations, String adep) {
        LocationOccupation occ = locations.get(adep);
        if (occ == null) {
            occ = new LocationOccupation();
            locations.put(adep, occ);
        }
        return occ;
    }

}
