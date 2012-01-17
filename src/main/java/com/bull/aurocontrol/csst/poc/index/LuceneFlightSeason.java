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
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.AtomicTaskFactory;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.Merger;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class LuceneFlightSeason implements FlightSeason {

    private Set<String> locations;

    private int periodDuration = 0;

    private IndexReader reader;

    private int[] uids;
    private Flight[] db;

    public LuceneFlightSeason(IndexReader reader, Set<String> locations, int periodDuration, int[] uids, Flight[] db) {
        super();
        this.reader = reader;

        this.locations = locations;
        this.periodDuration = periodDuration;
        this.uids = uids;
        this.db = db;
    }

    @Override
    public int getDuration() {
        return 0;
    }

    @Override
    public Date getFirstDayOfSeason() {
        return null;
    }


    @Override
    public Date getLastDayOfSeason() {
        return null;
    }

    @Override
    public int getNumberOfFlights() {
        return reader.numDocs();
    }

    @Override
    public long indexSize() {
        return ((RAMDirectory) reader.directory()).sizeInBytes();
    }

    @Override
    public SMatrix<Integer> queryOverlaps(int paralellism) throws IOException {
        Monitor mon = MonitorFactory.startPrimary("overlaps");

        ForkJoinPool pool = new ForkJoinPool(paralellism);
        String[] items = locations.toArray(new String[locations.size()]);
        Merger<SMatrix<Integer>> merger = new MutableIntMatrixAdder();
        AtomicTaskFactory<String, SMatrix<Integer>, LuceneFlightSeason> factory = new AtomicTaskFactory<String, SMatrix<Integer>, LuceneFlightSeason>() {

            @Override
            public RecursiveTask<SMatrix<Integer>> create(String item, int index, LuceneFlightSeason parameters) {
                return new ComputeLocationOverlapTime(item, parameters);
            }
        };
        
        MapReduceTask<String, SMatrix<Integer>, LuceneFlightSeason> task = new MapReduceTask<String, SMatrix<Integer>, LuceneFlightSeason>(items, this, merger, factory);
        
        SMatrix<Integer> overlaps = pool.submit(task).join();
        
        mon.stop();
        pool.shutdown();
        return overlaps;

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
        return this.db[this.uids[i]];
    }

}
