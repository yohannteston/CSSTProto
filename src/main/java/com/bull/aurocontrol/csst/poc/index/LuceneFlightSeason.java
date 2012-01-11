package com.bull.aurocontrol.csst.poc.index;

import java.util.Date;

import org.apache.lucene.index.IndexReader;

import com.bull.aurocontrol.csst.poc1.FlightSeason;

public class LuceneFlightSeason implements FlightSeason {

    public LuceneFlightSeason(IndexReader reader) {
        super();
        this.reader = reader;
    }

    IndexReader reader;

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
    
}
