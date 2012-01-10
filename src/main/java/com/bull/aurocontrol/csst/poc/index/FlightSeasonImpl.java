package com.bull.aurocontrol.csst.poc.index;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.bull.aurocontrol.csst.poc1.FlightSeason;

public class FlightSeasonImpl implements FlightSeason {
    private Map<String, LocationOccupation> locations;
    
    
    public FlightSeasonImpl(HashMap<String, LocationOccupation> locations) {
        this.locations = locations;
    }

    @Override
    public long indexSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getDuration() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Date getLastDayOfSeason() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getFirstDayOfSeason() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getNumberOfFlights() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String toString() {
        return "FlightSeasonImpl [locations=" + locations.size() + " : "+ locations.keySet()+"]";
    }
    
    

}
