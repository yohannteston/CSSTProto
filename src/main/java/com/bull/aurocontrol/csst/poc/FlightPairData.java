package com.bull.aurocontrol.csst.poc;

public class FlightPairData {
    private Flight a;
    private Flight b;
    private boolean anagram;
    private boolean identicalDigits;
    private boolean parallelCharacters;
    private int overlapTime;
    
    
    public FlightPairData(Flight a, Flight b, int overlapTime) {
        super();
        this.a = a;
        this.b = b;
        this.overlapTime = overlapTime;
    }


    public boolean isAnagram() {
        return anagram;
    }


    public void setAnagram(boolean anagram) {
        this.anagram = anagram;
    }


    public boolean isIdenticalDigits() {
        return identicalDigits;
    }


    public void setIdenticalDigits(boolean identicalDigits) {
        this.identicalDigits = identicalDigits;
    }


    public boolean isParallelCharacters() {
        return parallelCharacters;
    }


    public void setParallelCharacters(boolean parallelCharacters) {
        this.parallelCharacters = parallelCharacters;
    }


    @Override
    public String toString() {
        return "FlightPairData [a=" + a + ", b=" + b + ", anagram=" + anagram + ", identicalDigits=" + identicalDigits + ", parallelCharacters="
                + parallelCharacters + ", overlapTime=" + overlapTime + "]";
    }
    
    
    
}
