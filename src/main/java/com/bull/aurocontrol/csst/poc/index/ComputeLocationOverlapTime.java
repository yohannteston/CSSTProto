package com.bull.aurocontrol.csst.poc.index;



import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.NumericUtils;

import com.bull.eurocontrol.csst.poc.utils.Closeables;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.AtomicTaskFactory;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.Merger;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.IntArrayDocIdSet;


import jsr166y.RecursiveTask;

public class ComputeLocationOverlapTime extends RecursiveTask<SMatrix<Integer>> {
    private String location;

    private LuceneFlightSeason index;

    
    
    
    
    public ComputeLocationOverlapTime(String location, LuceneFlightSeason index) {
        super();
        this.location = location;
        this.index = index;
    }


    @Override
    protected SMatrix<Integer> compute() {
        if (location.equals("LFFFACTA")) {
            System.out.println(location);
        }
        PeriodOfOperationCount[] flightGroupsCounters = countGroupbyPeriodOfWeekAndDocSet(); 

        ArrayList<PeriodOfOperationCount> nonNulls = new ArrayList<ComputeLocationOverlapTime.PeriodOfOperationCount>();
        for (int i = 0; i < flightGroupsCounters.length; i++) {
            PeriodOfOperationCount e = flightGroupsCounters[i];
            if (e != null) nonNulls.add(e);
        }
        
        if (nonNulls.size() == 0) return null;
        
        flightGroupsCounters = nonNulls.toArray(new PeriodOfOperationCount[nonNulls.size()]);
        
        
        Merger<SMatrix<Integer>> merger = new MutableIntMatrixAdder();
        AtomicTaskFactory<PeriodOfOperationCount, SMatrix<Integer>, LuceneFlightSeason> atomicTaskFactory = new AtomicTaskFactory<PeriodOfOperationCount, SMatrix<Integer>, LuceneFlightSeason>() {

            @Override
            public RecursiveTask<SMatrix<Integer>> create(PeriodOfOperationCount item, int index, LuceneFlightSeason parameters) {
                return new ComputeLocationPeriodOverlapTime(location, parameters, item);
            }
        };
        
        MapReduceTask<PeriodOfOperationCount, SMatrix<Integer>, LuceneFlightSeason> mrTask;
        mrTask = new MapReduceTask<PeriodOfOperationCount, SMatrix<Integer>, LuceneFlightSeason>(flightGroupsCounters, index, merger, atomicTaskFactory);
        
        return mrTask.invoke();
    }

    public static class PeriodOfOperationCount {
        private int period;
        private Map<DocSet, MutableInt> counts = new Object2ObjectOpenCustomHashMap<DocSet, MutableInt>(DocSetKeyHashStrategy.INSTANCE);
        
        private PeriodOfOperationCount(int period) {
            super();
            this.period = period;
        }

        public int getPeriod() {
            return period;
        }

        public Map<DocSet, MutableInt> getCounts() {
            return counts;
        }
        
    }
    
    
    private PeriodOfOperationCount[] countGroupbyPeriodOfWeekAndDocSet() {
        int totalPeriods = LuceneIndexSeasonFactory.MINUTE_PER_WEEK / index.getPeriodDuration();
        PeriodOfOperationCount[] result = new PeriodOfOperationCount[totalPeriods];

        IndexReader reader = index.getIndexReader();

        int[] docBuffer = new int[128];
        int[] otherBuffer = new int[128];
        TermDocs termDocs = null;
        TermEnum seasonPeriods = null;
        try {
            termDocs = reader.termDocs();
            seasonPeriods = reader.terms(new Term(location));

            do {
                Term term = seasonPeriods.term();
                if (term == null || !term.field().equals(location))
                    break;
                if (seasonPeriods.docFreq() > 1) {
                    termDocs.seek(seasonPeriods);

                    int p = NumericUtils.prefixCodedToInt(term.text()) % totalPeriods;
//                    DocSet d = DocSetFactory.getDocSetInstance(0, reader.maxDoc(), seasonPeriods.docFreq(), FOCUS.PERFORMANCE);
                    DocSet d = new IntArrayDocIdSet(seasonPeriods.docFreq());
                    
                    
                    while (true) {
                        final int count = termDocs.read(docBuffer, otherBuffer);
                        if (count != 0) {
                            d.addDocs(docBuffer, 0, count);
                        } else {
                            break;
                        }
                    }

                    PeriodOfOperationCount m = result[p];
                    if (m == null) {
                        result[p] = m = new PeriodOfOperationCount(p);
                        m.counts.put(d, new MutableInt(1));
                    } else {
                        MutableInt counter = m.counts.get(d);
                        if (counter == null) {
                            m.counts.put(d, new MutableInt(1));
                        } else {
                            counter.increment();
                        }
                    }


                }                    
            } while (seasonPeriods.next());


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Closeables.closeSilently(seasonPeriods);
            Closeables.closeSilently(termDocs);
        }


        return result;
    }
    
    
}
