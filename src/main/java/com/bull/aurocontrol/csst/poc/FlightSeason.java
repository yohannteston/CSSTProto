package com.bull.aurocontrol.csst.poc;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.mutable.MutableInt;

import com.bull.eurocontrol.csst.poc.utils.SMatrix;


public interface FlightSeason {

    SMatrix<FlightPairData> queryConflicts() throws IOException;

    SMatrix<FlightPairData> queryConflicts(ConflictQuery query) throws IOException;

    Flight getFlight(int i);
    
    

}
