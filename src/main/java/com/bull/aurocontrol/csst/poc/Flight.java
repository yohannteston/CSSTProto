package com.bull.aurocontrol.csst.poc;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;



public class Flight {

    private int id;
    private String cfn;
    private String atcOperator;
    private String atcFlightId;
    private String departureAirport;
    private String destinationAirport;
    private int eobt;
    private int eta;    
    private FlightProfile profile;
    
    
    private FlightSchedule[] schedules;


    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getAtcFlightId() {
        return atcFlightId;
    }
    public void setAtcFlightId(String atcFlightId) {
        this.atcFlightId = atcFlightId;
    }

    public String getDepartureAirport() {
        return departureAirport;
    }
    public void setDepartureAirport(String departureAirport) {
        this.departureAirport = departureAirport;
    }
    public String getDestinationAirport() {
        return destinationAirport;
    }
    public void setDestinationAirport(String destinationAirport) {
        this.destinationAirport = destinationAirport;
    }
    public String getAtcOperator() {
        return atcOperator;
    }
    public void setAtcOperator(String operator) {
        this.atcOperator = operator;
    }
    
    public FlightProfile getProfile() {
        return profile;
    }
    public void setProfile(FlightProfile profile) {
        this.profile = profile;
    }
    public int countAirspace() {
        return (profile == null) ? 0 : profile.countAirspaces();
    }
    
    public int getEobt() {
        return eobt;
    }

    public void setEobt(int eobt) {
        this.eobt = eobt;
    }

    public int getEta() {
        return eta;
    }

    public void setEta(int eta) {
        this.eta = eta;
    }

    public AirspaceProfile getAirspace(int j) {
        
        return profile.getAirspaces()[j];
    }
    
    public String[] getAirspaces() {
        if (profile == null) return new String[0];
        String[] r = new String[profile.getAirspaces().length];
        for (int i = 0; i < r.length; i++) {
            r[i] = profile.getAirspaces()[i].getName();
        }
        return r;
    }
    public String getCfn() {
        return cfn;
    }
    public void setCfn(String cfn) {
        this.cfn = cfn;
    }
    public FlightSchedule[] getSchedules() {
        return schedules;
    }
    public void setSchedules(FlightSchedule[] schedules) {
        this.schedules = schedules;
    }
    
    
    
    public long getFirstDayOfOperation() {
        long d = Long.MAX_VALUE;
        for (FlightSchedule s : schedules) {
            if (s.getFirstDayOfOperation() < d) {
                d = s.getFirstDayOfOperation();
            }
        }
        return d;
    }
    public long getLastDayOfOperation() {
        long d = Long.MIN_VALUE;
        for (FlightSchedule s : schedules) {
            if (s.getLastDayOfOperation() > d) {
                d = s.getLastDayOfOperation();
            }
        }
        return d;
    }
    @Override
    public String toString() {
        return "Flight [id=" + id + ", cfn=" + cfn + ", atcOperator=" + atcOperator + ", atcFlightId=" + atcFlightId + ", departureAirport=" + departureAirport
                + ", destinationAirport=" + destinationAirport + ", eobt=" + eobt + ", eta=" + eta + ", \n\tprofile=" + profile + ", \n\tschedules="
                + StringUtils.join(schedules, ",\n\t\t") + "]";
    }
    
    
}
