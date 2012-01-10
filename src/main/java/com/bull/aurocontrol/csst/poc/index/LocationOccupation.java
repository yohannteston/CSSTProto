package com.bull.aurocontrol.csst.poc.index;

import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;

import com.bull.aurocontrol.csst.poc1.Flight;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.IntArrayDocIdSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.kamikaze.docidset.utils.DocSetFactory.FOCUS;

public class LocationOccupation {
   


    private int periodDuration = 120;
    
    private static class OccupationPeriod {
        
        private DocSet starting =  new IntArrayDocIdSet(10);
        private DocSet ending = new IntArrayDocIdSet(10);
        private DocSet overlapping= new IntArrayDocIdSet(10);
        
        
        public void addStarting(int id) {
            if (starting == null) {
                starting =  new IntArrayDocIdSet(1);
            }
            try {
                starting.addDoc(id);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        public void addOverlap(int id) {
            if (overlapping == null) {
                overlapping =  new IntArrayDocIdSet(1);
            }
            try {
                overlapping.addDoc(id);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        public void addEnding(int id) {
            if (ending == null) {
                ending =  new IntArrayDocIdSet(1);
            }
            try {
                ending.addDoc(id);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        @Override
        public String toString() {
            
            
            return "OccupationPeriod [starting=" + LocationOccupation.toString(starting) + ", ending=" + LocationOccupation.toString(ending) + ", overlapping=" + LocationOccupation.toString(overlapping) + "]";
        }
        public void optimize() {
            try {
                starting.optimize();
                ending.optimize();
                overlapping.optimize();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
        
    }
    
    private Int2ObjectRBTreeMap<OccupationPeriod> index = new Int2ObjectRBTreeMap<OccupationPeriod>();
    
    
    public LocationOccupation() {
        super();
    }


    public void add(int flight, int departureTimeLow, int departureTimeHigh) {
        
        int firstPeriod = departureTimeLow / periodDuration;
        int lastPeriod = departureTimeHigh / periodDuration + 1;
        
        getOrCreatePeriod(firstPeriod).addStarting(flight);
        getOrCreatePeriod(lastPeriod).addEnding(flight);

        for (int i = firstPeriod+1; i < lastPeriod; i++) {
            getOrCreatePeriod(i).addOverlap(flight);
        }
    }


    private OccupationPeriod getOrCreatePeriod(int firstPeriod) {
        OccupationPeriod p = index.get(firstPeriod);
        if (p == null) {
            p = new OccupationPeriod();
            index.put(firstPeriod, p);
        }
        return p;
    }


    @Override
    public String toString() {
        return "LocationOccupation [periodDuration=" + periodDuration + ", index=" + index + "]";
    }
    
    private static String toString(DocSet set) {
        StringBuilder bitString = new StringBuilder("{");
        try {
            DocIdSetIterator iter = set.iterator();
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                bitString.append(iter.docID());
                bitString.append(',');
                
            }
            if (bitString.length() > 1)
                bitString.setCharAt(bitString.length()-1, '}');
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return bitString.toString();
    }
    
    public void optimize() {
        for (OccupationPeriod p : index.values()) {
            p.optimize();
        }
    }
}
