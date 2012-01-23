package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import jsr166y.RecursiveTask;

import org.apache.commons.collections.Closure;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.NumericUtils;

import com.bull.aurocontrol.csst.poc.index.ComputeLocationOverlapTime.PeriodOfOperationCount;
import com.bull.eurocontrol.csst.poc.utils.Closeables;
import com.bull.eurocontrol.csst.poc.utils.IClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.SVector;
import com.kamikaze.docidset.api.DocSet;

public class ComputeLocationPeriodOverlapTime extends RecursiveTask<SMatrix<Integer>> {
    private String location;
    private int period;
    private OverlapProcessParameters index;
    private PeriodOfOperationCount flightGroupsCounter;
    private boolean debug = false;



    public ComputeLocationPeriodOverlapTime(String location, OverlapProcessParameters index, PeriodOfOperationCount flightGroupsCounter) {
        super();
        this.location = location;
        this.period = flightGroupsCounter.getPeriod();
        this.index = index;
        this.flightGroupsCounter = flightGroupsCounter;
    }

    private static class FlightPairCounter {
        int overlapTimeInPeriod;
        int numberOfPeriodInSeason = 0;

        public FlightPairCounter(int overlapTimeInPeriod) {
            super();
            this.overlapTimeInPeriod = overlapTimeInPeriod;
        }

        @Override
        public String toString() {
            return "FlightPairCounter [overlapTimeInPeriod=" + overlapTimeInPeriod + ", numberOfPeriodInSeason=" + numberOfPeriodInSeason + "]";
        }

        public int getTotal() {
            return overlapTimeInPeriod * numberOfPeriodInSeason;
        }



    }




    @Override
    protected SMatrix<Integer> compute() {
        final String prefix = "week_" + location + "_" + NumericUtils.intToPrefixCoded(period);
        final IndexReader reader = index.getIndexReader();
       
//        if (location.equals("EFTUTMA") && period == 1) debug = true;
        if (debug) {
            
            System.out.println(location + " " + period + ": start ->");
            System.out.println("break");
        }
        
        int[] docBuffer = new int[reader.maxDoc()];
        int[] otherBuffer = new int[reader.maxDoc()];
        int[] uids = index.getUIDS(); 
        TermDocs termDocs = null;
        TermEnum weeksPeriods = null;
        try {
            termDocs = reader.termDocs();
            weeksPeriods = reader.terms(new Term(prefix));
            Term term = weeksPeriods.term();
            if (term == null || !term.field().startsWith(prefix)) return null;

            boolean donePeriod = false;

            Int2ObjectSortedMap<Int2ObjectSortedMap<FlightPairCounter>> periodFlightPairCounters = new Int2ObjectRBTreeMap<Int2ObjectSortedMap<FlightPairCounter>>();
            Int2IntMap periodFlights = new Int2IntRBTreeMap();

            do {
                String tsKey = term.text();

                // load posting list
                termDocs.seek(weeksPeriods);
                int n = termDocs.read(docBuffer, otherBuffer);
                if (n != weeksPeriods.docFreq()) {
                    System.out.println("lucene problem");
                }
                // map to uids
                for (int i = 0; i < n; i++) {
                    docBuffer[i] = uids[docBuffer[i]];
                }
                IntArrays.quickSort(docBuffer, 0, n);
                
                // process posting list
                processTsEntry(tsKey, docBuffer, otherBuffer, n, periodFlights, periodFlightPairCounters);

                // move to next entry
                if (weeksPeriods.next()) {
                    term = weeksPeriods.term();

                    if (!term.field().startsWith(prefix)) {
                        donePeriod = true;
                    }
                } else {
                    donePeriod = true;
                }
            } while (!donePeriod);

            if (periodFlights.size() > 1) {
                processRemainingFlightsAtEndOfPeriod(periodFlightPairCounters, periodFlights, docBuffer, otherBuffer);
            }    


            if (periodFlightPairCounters.size() > 0) {
                //System.out.println(periodFlightPairCounters.size() + " : " + flightGroupsCounter.getCounts().size());

                SMatrix<FlightPairCounter> compressed = new SMatrix<FlightPairCounter>(periodFlightPairCounters);

                updatePeriodCounterWithSeasonPeriodCounts(compressed, flightGroupsCounter);
                SMatrix<Integer> weekFlightPairCounters = compressed.transform(new IJTransformer<FlightPairCounter,Integer>() {

                    @Override
                    public Integer transform(int i, int j, FlightPairCounter val) {
                        int total = val.getTotal();
                        if (total == 0) return null;
                        return Integer.valueOf(total);
                    }

                });
                if (debug ) {
                    System.out.println(location + " " + period + ": done ->");
                    if ((period == 1) || period == 3) {
                        if (debug) System.out.println(weekFlightPairCounters);
                        
                    }
                    
                }
                return weekFlightPairCounters;
            }

        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeSilently(weeksPeriods);
            Closeables.closeSilently(termDocs);
        }
        return null;
    }

    private void processTsEntry(String tsKey, int[] docs, int[] tmpBuffer, final int n, Int2IntMap currentFlights, Int2ObjectSortedMap<Int2ObjectSortedMap<FlightPairCounter>> weekCounters) {
        if (tsKey.charAt(0) == '0') { // overlapping flights
            if (debug)
                System.out.println(": overlapping  -> " + Arrays.toString(ArrayUtils.subarray(docs,0,n)));
            addAll(docs, n, currentFlights, 0);
        } else {
            int len = tsKey.length();
            int ts = NumericUtils.prefixCodedToInt(tsKey.substring(0,len-2));
            int[] uids = index.getUIDS();

            if (tsKey.charAt(len-1) == 'a') { // arriving flights 
                if (debug) {
                    System.out.println(": arr at " + ts + " -> " + Arrays.toString(ArrayUtils.subarray(docs,0,n)));
                    System.out.println("before: " + currentFlights);
                }
                addAll(docs, n, currentFlights, ts);
                if (debug) {
                    System.out.println("after: " + currentFlights);
                }
            } else { // leaving flights
                if (debug) {
                    System.out.println(": rem at " + ts + " -> " + Arrays.toString(ArrayUtils.subarray(docs,0,n)));
                    System.out.println("before: " + currentFlights);
                }
                for (int i = 0; i < n; i++) {
                    tmpBuffer[i] = currentFlights.get(docs[i]);
                }
                
                ObjectSortedSet<Entry> remaining = (ObjectSortedSet<Entry>)currentFlights.int2IntEntrySet();
                Iterator<Entry> iterator = remaining.iterator();
                int i = 0;
                int doci = docs[i];
                while (iterator.hasNext()) {
                    Entry ej = (Entry) iterator.next();
                    int docj = ej.getIntKey();
                    if (docj < doci) {
                        Int2ObjectSortedMap<FlightPairCounter> row = getOrCreateRow(weekCounters, docj);
                        int startj = ej.getIntValue();

                        for (int j = i; j < n; j++) {
                            int doca = docs[j];
                            int starta = tmpBuffer[j];

                            if (uids[docj] != uids[doca]) row.put(doca, new FlightPairCounter(ts - Math.max(startj, starta)));                                
                        }                            
                    } else {
                        if (docj != doci) {
                            System.out.println("bug " + doci + " - " + docj);
                        }
                        assert docj == doci;
                        int starti = ej.getIntValue();
                        if (docj == doci) {
                            iterator.remove();
                        }

                        if (iterator.hasNext()) {
                            Int2ObjectSortedMap<FlightPairCounter> row = getOrCreateRow(weekCounters, doci);

                            for (Iterator<Entry> rest = remaining.iterator(ej); rest.hasNext();) {
                                Entry a = rest.next();
                                int doca = a.getIntKey();
                                int starta = a.getIntValue();

                                if (uids[docj] != uids[doca]) row.put(doca, new FlightPairCounter(ts - Math.max(starti, starta)));

                            }
                            i++;

                            if (i >= n) break;
                            doci = docs[i];
                        } else {
                            break;
                        }
                    }
                }
                if (debug) {
                    System.out.println("after: " + currentFlights);
                }
                

            }

        }
    }

    private void processRemainingFlightsAtEndOfPeriod(Int2ObjectSortedMap<Int2ObjectSortedMap<FlightPairCounter>> weekCounters, Int2IntMap currentFlights, int[] docs, int[] tmp) {
        int[] uids = index.getUIDS();
        int periodDuration = index.getPeriodDuration();
        int n = 0;

        // transfer to array for fast iteration
        for (Iterator<Entry> iterator = currentFlights.int2IntEntrySet().iterator(); iterator.hasNext();) {
            Entry ej = (Entry) iterator.next();
            docs[n] = ej.getIntKey();
            tmp[n++] = ej.getIntValue();
        }

        //                System.out.println(": remaining at end " + periodDuration + " -> " + Arrays.toString(docs));

        // compute end of period overlaps
        for (int i = 0, endi = n-1; i < endi; i++) {
            int doci = docs[i];
            int starti = tmp[i];
            Int2ObjectSortedMap<FlightPairCounter> row = getOrCreateRow(weekCounters, doci);

            for (int j = i+1; j < n; j++) {
                int docj = docs[j];
                if (uids[doci] != uids[docj]) {
                    row.put(docj, new FlightPairCounter(periodDuration - Math.max(starti, tmp[j])));
                }
            }
        }
        //    System.out.println(weekCounters);

    }
    private void updatePeriodCounterWithSeasonPeriodCounts(SMatrix<FlightPairCounter> compressed,
            PeriodOfOperationCount flightGroupsCounter) throws IOException {
        for (Map.Entry<DocSet, MutableInt> c : flightGroupsCounter.getCounts().entrySet()) {
            mixPeriodCountAndWeekOverlaps(compressed, c);

            //System.out.printf("mixed %s -> %s\n", periodInWeek, overlaps);
        }
    }





    private void mixPeriodCountAndWeekOverlaps(SMatrix<FlightPairCounter> compressed, Map.Entry<DocSet, MutableInt> c) throws IOException {
        final DocSet set = c.getKey();
        final int count = c.getValue().intValue();

        compressed.executeOnEachRow(set, new IClosure<SVector<FlightPairCounter>>() {

            @Override
            public void execute(int i, SVector<FlightPairCounter> row) {
                row.executeOnEachItem(set, new IClosure<FlightPairCounter>() {

                    @Override
                    public void execute(int i, FlightPairCounter c) {
                        c.numberOfPeriodInSeason += count; 
                    }

                });
            }

        });

    }


    private static void addAll(int[] docs, final int n, Int2IntMap currentFlights, int ts) {
        for (int i = 0; i < n; i++) {
            int doci = docs[i];
            currentFlights.put(doci, ts);
        }
    }
    private Int2ObjectSortedMap<FlightPairCounter> getOrCreateRow(Int2ObjectSortedMap<Int2ObjectSortedMap<FlightPairCounter>> weekCounters, int i) {
        Int2ObjectSortedMap<FlightPairCounter> row = weekCounters.get(i);
        if (row == null) {
            row = new Int2ObjectRBTreeMap<FlightPairCounter>();
            weekCounters.put(i,row);
        }
        return row;
    }

}
