package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;

import java.util.Iterator;

public class Int2ObjectSortedIterator<V> implements Iterator<Int2ObjectMap.Entry<V>> {
    private Iterator<Int2ObjectMap.Entry<V>> iter ;
    private boolean advanced = false;
    private Int2ObjectMap.Entry<V> currentAfterAdvance;
    
    public Int2ObjectSortedIterator(Int2ObjectMap<V> row, int start) {
        super();
        iter = ((Int2ObjectMap.FastEntrySet<V>)row.int2ObjectEntrySet()).fastIterator();                
    }

    public void advance(final int k) {
        while (hasNext()) {
            Int2ObjectMap.Entry<V> e = next();
            if (e.getIntKey() >= k) {
                currentAfterAdvance = e;
                advanced = true;
                break;
            }
        }
        
    }
    
    
    @Override
    public boolean hasNext() {
        if (advanced) return true;
        return iter.hasNext();
    }


    @Override
    public Int2ObjectMap.Entry<V> next() {
        if (advanced) {
            advanced = false;
            return currentAfterAdvance;
        }
        return iter.next();
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    
    
    
}