package com.bull.aurocontrol.csst.poc.index;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

import jsr166y.ForkJoinPool;
import jsr166y.RecursiveTask;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.RAMDirectory;

import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.AtomicTaskFactory;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.Merger;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class OverlapProcessParameters {

    private Set<String> locations;

    private int periodDuration = 0;

    private IndexReader reader;

    private int[] uids;
    private Flight[] db;

    private SMatrix<Integer> overlaps;

    public OverlapProcessParameters(IndexReader reader, Set<String> locations, int periodDuration, int[] uids, Flight[] db) {
        super();
        this.reader = reader;

        this.locations = locations;
        this.periodDuration = periodDuration;
        this.uids = uids;
        this.db = db;
    }


    public int getPeriodDuration() {
        return periodDuration;
    }
    public IndexReader getIndexReader() {
        return reader;
    }
    public int[] getUIDS() {
        return uids;
    }

    public Flight getFlight(int i) {
        return this.db[i];
    }

    public void setOverlapsIndex(SMatrix<Integer> overlaps) {
        this.overlaps = overlaps;
        reader = null;
        
    }

    public SMatrix<Integer> getOverlapMatrix() {
        return overlaps;
    }

}
