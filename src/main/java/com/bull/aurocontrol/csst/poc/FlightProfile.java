package com.bull.aurocontrol.csst.poc;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class FlightProfile {

    private int duration;
    private int taxitime;
    private AirspaceProfile[] airspaces;
    
    public FlightProfile(int duration, int taxitime, AirspaceProfile[] airspaces) {
        super();
        this.duration = duration;
        this.airspaces = airspaces;
        this.taxitime = taxitime;
    }

    @Override
    public String toString() {
        return "FlightProfile [duration=" + duration + ", \n\t\tairspaces=" + StringUtils.join(airspaces, ",\n\t\t\t") + "]";
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

    public int getTaxitime() {
        return taxitime;
    }

    public void setTaxitime(int taxitime) {
        this.taxitime = taxitime;
    }
    
    
    
}
