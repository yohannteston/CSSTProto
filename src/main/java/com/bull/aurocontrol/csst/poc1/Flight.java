package com.bull.aurocontrol.csst.poc1;

import java.util.Date;
import java.util.EnumSet;



public class Flight {

    private int id;
    private String atcOperator;
    private String atcFlightId;
    private Date firstDayOfOperation;
    private Date lastDayOfOperation;
    private EnumSet<DayOfWeek> daysOfOperation;
    private int eobt; // in minute since midnight the day of operation
    private int eta; // in minute since midnight the day of operation
    private String departureAirport;
    private String destinationAirport;
    private FlightProfile profile;
    
    
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
    public Date getFirstDayOfOperation() {
        return firstDayOfOperation;
    }
    public void setFirstDayOfOperation(Date firstDayOfOperation) {
        this.firstDayOfOperation = firstDayOfOperation;
    }
    public Date getLastDayOfOperation() {
        return lastDayOfOperation;
    }
    public void setLastDayOfOperation(Date lastDayOfOperation) {
        this.lastDayOfOperation = lastDayOfOperation;
    }
    public EnumSet<DayOfWeek> getDaysOfOperation() {
        return daysOfOperation;
    }
    public void setDaysOfOperation(EnumSet<DayOfWeek> daysOfOperation) {
        this.daysOfOperation = daysOfOperation;
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
    @Override
    public String toString() {
        return "Flight [id=" + id + ", atcOperator=" + atcOperator + ", atcFlightId=" + atcFlightId + ", firstDayOfOperation=" + firstDayOfOperation
                + ", lastDayOfOperation=" + lastDayOfOperation + ", daysOfOperation=" + daysOfOperation + ", eobt=" + eobt + ", eta=" + eta
                + ", departureAirport=" + departureAirport + ", destinationAirport=" + destinationAirport + ", profile=" + profile + "]";
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
    
    public int getStart(int airspace) {
        int realDur = eta - eobt;
        AirspaceProfile airspaceProfile = profile.getAirspaces()[airspace];
        return airspaceProfile.getStart() * realDur / profile.getDuration();
        
    }
    public int getEnd(int airspace) {
        int realDur = eta - eobt;
        AirspaceProfile airspaceProfile = profile.getAirspaces()[airspace];
        return airspaceProfile.getEnd() * realDur / profile.getDuration();
        
    }
    public String getAirspace(int j) {
        
        return profile.getAirspaces()[j].getName();
    }
    
    
}
