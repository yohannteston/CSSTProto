package com.bull.eurocontrol.csst.poc.utils;


import javax.sql.rowset.Predicate;

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang3.mutable.MutableInt;

import com.kamikaze.docidset.api.DocSet;

import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;

public class SMatrix<V>   {
    private SVector<SVector<V>> tree;
    private int size = -1;

    public SMatrix(Int2ObjectSortedMap<Int2ObjectSortedMap<V>> m) {
        tree = new SVector<SVector<V>>(m, new Transformer() {
            @Override
            public Object transform(Object input) {                
                
                SVector<V> sVector = new SVector<V>((Int2ObjectSortedMap<V>) input);
                if (sVector.indexes().length == 0) return null;
                return  sVector;
            }

        });        
    }

    public SMatrix(SVector<SVector<V>> newTree) {
        this.tree = newTree;
    }

    public void executeOnEachRow(DocSet set, Closure closure) {
        tree.executeOnEachItem(set, closure);        
    }
    

    public static <V> SMatrix<V> combine(SMatrix<V> first, SMatrix<V> rest, final Combiner<V> combiner) {
        SVector<SVector<V>> newTree = 
        SVector.combine(first.tree, rest.tree, new Combiner<SVector<V>>() {

            @Override
            public SVector<V> combine(SVector<V> a, SVector<V> b) {
                
                return SVector.combine(a, b, combiner);
            }
            
        });
        
        return new SMatrix<V>(newTree);
    }

    public <T> SMatrix<T> transform(final IJTransformer<V,T> ijTransformer) {
         SVector<SVector<T>> newTree = tree.transform(new ITransformer<SVector<V>,SVector<T>>() {

            @Override
            public SVector<T> transform(final int i, final SVector<V> input) {
                return input.transform(new ITransformer<V,T>() {
                                        
                    @Override
                    public T transform(int j, V val) {
                        return ijTransformer.transform(i, j, val);
                    }
                }, true); 
                
            }
            
        }, false);

        return new SMatrix<T>(newTree);
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        
        int[] rowIndexes = tree.indexes();
        Object[] rowValues = tree.values();
        for (int i = 0; i < rowIndexes.length; i++) {
            b.append(rowIndexes[i]);
            b.append(":[");
            SVector<V> row = (SVector<V>) rowValues[i];
            int[] colIndexes = row.indexes();
            Object[] colValues = row.values();
            
            for (int j = 0; j < colIndexes.length; j++) {
                b.append(colIndexes[j]);
                b.append("=>");
                b.append(colValues[j]);
                b.append(",");
            }
            b.setCharAt(b.length()-1, ']');
            b.append('\n');
        }
        
        return b.toString();
    }

    public int size() {
        if (size == -1) {
            final MutableInt counter = new MutableInt();
            tree.execute(new IClosure<SVector<V>>() {

                @Override
                public void execute(int i, SVector<V> input) {
                    counter.add(input.size());
                }
                
            });
            size = counter.intValue();
        }
        
        return size;
    }

    public void execute(final IJClosure<V> ijClosure) {
        tree.execute(new IClosure<SVector<V>>() {

            @Override
            public void execute(final int i, final SVector<V> input) {
                input.execute(new IClosure<V>() {

                    @Override
                    public void execute(final int j, final V input) {
                        ijClosure.execute(i, j, input);
                    }
                    
                });
            }
            
        });
        
    }

}
