package com.bull.aurocontrol.csst.poc1;

import java.util.Arrays;

public class FlightProfile {

    private int duration;
    private AirspaceProfile[] airspaces;
    
    public FlightProfile(int duration, AirspaceProfile[] airspaces) {
        super();
        this.duration = duration;
        this.airspaces = airspaces;
    }

    @Override
    public String toString() {
        return "FlightProfile [duration=" + duration + ", airspaces=" + Arrays.toString(airspaces) + "]";
    }
    
    public int countAirspaces() {
        return (airspaces == null) ? 0 : airspaces.length;
    }

    public AirspaceProfile[] getAirspaces() {
        return airspaces;
    }

    public void setAirspaces(AirspaceProfile[] airspaces) {
        this.airspaces = airspaces;
    }

    public int getDuration() {
        return duration;
    }
    
    
    
}
