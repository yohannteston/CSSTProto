package com.bull.aurocontrol.csst.poc.index.rules;

import java.util.Arrays;

import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightSeason;


/**
 * AnagramsRule.
 * 
 * @author pcantalo
 */
public class AnagramsRule {
   
    /**
     * ATC flight ids cannot be full anagrams of each other (i.e. exact same characters in different order).
     * 
     * @param flightPair the flightPair to check
     * @return true or false
     */

    
    public boolean check(final Flight f1,  final Flight f2) {
//        if (f1.getCfn().equals("006") && f2.getCfn().equals("1064")) {
//            System.out.println("rule check");
//        }

        boolean success = false;

        char[] charAtcId1 = f1.getAtcFlightId().toCharArray();
        
        char[] charAtcId2 = f2.getAtcFlightId().toCharArray();
        int length1 = charAtcId1.length;
        int length2 = charAtcId2.length;
        
        for (int i1 = 0; i1 < length1; i1++) {
            boolean found = false;
            for (int i2 = 0; i2 < length2; i2++) {
                if (charAtcId1[i1]==charAtcId2[i2]) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                success = true;
                break;                
            }
        }
        if (success == false) {
            for (int i2 = 0; i2 < length2; i2++) {
                boolean found = false;
                for (int i1 = 0; i1 < length1; i1++) {
                    if (charAtcId1[i1]==charAtcId2[i2]) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    success = true;
                    break;                
                }
            }
        }

        return success;
    }

  
}
