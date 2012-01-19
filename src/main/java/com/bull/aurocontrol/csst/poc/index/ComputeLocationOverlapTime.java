package com.bull.aurocontrol.csst.poc.index;



import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import jsr166y.RecursiveTask;

import org.apache.commons.collections.map.IdentityMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;

import com.bull.eurocontrol.csst.poc.utils.Closeables;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.AtomicTaskFactory;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.Merger;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.ImmutableIntArrayDocIdSet;
import com.kamikaze.docidset.impl.IntArrayDocIdSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.kamikaze.docidset.utils.IntArray;

public class ComputeLocationOverlapTime extends RecursiveTask<SMatrix<Integer>> {
    private String location;

    private OverlapProcessParameters index;

    
    
    
    
    public ComputeLocationOverlapTime(String location, OverlapProcessParameters index) {
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
            if (e != null) {
                e.convertToUIDs(index.getUIDS());
                nonNulls.add(e);
            }
        }
        
        if (nonNulls.size() == 0) return null;
        
        flightGroupsCounters = nonNulls.toArray(new PeriodOfOperationCount[nonNulls.size()]);
        
        
        Merger<SMatrix<Integer>> merger = new IntMatrixAdder();
        AtomicTaskFactory<PeriodOfOperationCount, SMatrix<Integer>, OverlapProcessParameters> atomicTaskFactory = new AtomicTaskFactory<PeriodOfOperationCount, SMatrix<Integer>, OverlapProcessParameters>() {

            @Override
            public RecursiveTask<SMatrix<Integer>> create(PeriodOfOperationCount item, int index, OverlapProcessParameters parameters) {
                return new ComputeLocationPeriodOverlapTime(location, parameters, item);
            }
        };
        
        MapReduceTask<PeriodOfOperationCount, SMatrix<Integer>, OverlapProcessParameters> mrTask;
        mrTask = new MapReduceTask<PeriodOfOperationCount, SMatrix<Integer>, OverlapProcessParameters>(flightGroupsCounters, index, merger, atomicTaskFactory);
        
        return mrTask.invoke();
    }

    public static class PeriodOfOperationCount {
        private int period;
        private Map<DocSet, MutableInt> counts = new Object2ObjectOpenCustomHashMap<DocSet, MutableInt>(DocSetKeyHashStrategy.INSTANCE);
        
        private PeriodOfOperationCount(int period) {
            super();
            this.period = period;
        }

        public void convertToUIDs(int[] uids) {
            Map<DocSet, MutableInt> newCounts = new IdentityHashMap<DocSet, MutableInt>(counts.size());
            
            try {
                for (Map.Entry<DocSet, MutableInt> e : counts.entrySet()) {
                      
                    IntRBTreeSet nDocSet = new IntRBTreeSet();
                    int i = 0;
                    DocIdSetIterator it = e.getKey().iterator();
                    
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc(), i++) {
                        nDocSet.add(uids[doc]);
                    }
                    DocSet set = new IntArrayDocIdSet(nDocSet.size());
                    for (IntIterator iterator = nDocSet.iterator(); iterator.hasNext();) {
                        set.addDoc(iterator.nextInt());                        
                    }
                    newCounts.put(set, e.getValue());
                }
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
            this.counts = newCounts;
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
