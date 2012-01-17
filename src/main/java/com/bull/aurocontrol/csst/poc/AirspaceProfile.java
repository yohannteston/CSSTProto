package com.bull.aurocontrol.csst.poc;

public class AirspaceProfile {
    private String name;
    private int start;
    private int duration;
    
    
    
    public AirspaceProfile(String name, int start, int duration) {
        super();
        this.name = name;
        this.start = start;
        this.duration = duration;
    }
    
    public String getName() {
        return name;
    }
    public int getStart() {
        return start;
    }
    public int getDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return "AirspaceProfile [name=" + name + ", start=" + start + ", duration=" + duration + "]";
    }

    public int getEnd() {
        return start + duration;
    }
    
    
}
