package com.bull.eurocontrol.csst.poc.utils;

import java.io.IOException;
import java.util.Arrays;



import org.apache.commons.collections.Closure;
import org.apache.commons.collections.Factory;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.TransformerUtils;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.api.StatefulDSIterator;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrays;

public class SVector<V>  {
    private int[] index;
    private V[] value;



    public SVector(Int2ObjectSortedMap<V> items) {
        this(items, TransformerUtils.nopTransformer());
    }

    public <F> SVector(Int2ObjectSortedMap<F> items, Transformer converter) {
        int len = items.size();
        index = new int[len];
        value = (V[]) new Object[len];

        int i = 0;
        for (Int2ObjectMap.Entry<F> e : items.int2ObjectEntrySet()) {
            V value2 = (V) converter.transform(e.getValue());

            if (value2 != null) {
                index[i] = e.getIntKey();
                value[i] = value2;
                i++;
            }
        }

        index = IntArrays.trim(index, i);
        value = ObjectArrays.trim(value, i);
    }

    private SVector(int[] index, V[] vals) {
        this.index = index;
        this.value = vals;
    }

    public SVector(DocIdSet set, int size, Factory factory) {
        try {
            this.index = new int[size];
            DocIdSetIterator it = set.iterator();
            int j = 0;
            for (int i = it.nextDoc(); i != DocIdSetIterator.NO_MORE_DOCS; i = it.nextDoc(), j++) {
                this.index[j] = i;
                this.value[j] = (V) factory.create();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void executeOnEachItem(DocIdSet set, IClosure<V> closure) {
        try {
            DocIdSetIterator matchIndex = set.iterator();
            StatefulDSIterator rowIndex = new ImmutableIntArrayDocIdSetIterator(index);

            int j = rowIndex.nextDoc();
            int i = matchIndex.advance(j);
            while (i != DocIdSetIterator.NO_MORE_DOCS && j != DocIdSetIterator.NO_MORE_DOCS) {
                if (i == j) {
                    int cursor = rowIndex.getCursor();
                    closure.execute(index[cursor], value[cursor]);
                    j = rowIndex.nextDoc();
                    i = matchIndex.advance(j);
                } else if (i < j) {
                    i = matchIndex.advance(j);
                } else {
                    j = rowIndex.advance(i);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(IClosure<V> closure) {
        for (int i = 0; i < index.length; i++) {
            closure.execute(index[i], value[i]);
        }
    }




    public final static class ImmutableIntArrayDocIdSetIterator extends StatefulDSIterator {
        private int _doc;
        private int cursor;
        private final int[] _array;

        public ImmutableIntArrayDocIdSetIterator(int[] array){
            _array=array;
            _doc = -1;
            cursor=-1;
        }

        @Override
        final public int docID(){
            return _doc;
        }

        @Override
        public int nextDoc() throws java.io.IOException{
            if (++cursor < _array.length) {
                _doc = _array[cursor];
            }
            else{
                _doc = DocIdSetIterator.NO_MORE_DOCS;
            }
            return _doc;
        }

        @Override
        public int advance(int target) throws java.io.IOException{
            if (cursor >= _array.length || _array.length == -1) return DocIdSetIterator.NO_MORE_DOCS;
            if (target <= _doc) target = _doc + 1;      
            int index = Arrays.binarySearch(_array, target);
            if (index > 0){
                cursor = index;
                _doc = _array[cursor];
                return _doc;
            }
            else{
                cursor = -(index+1);
                if (cursor >= _array.length) {
                    _doc = DocIdSetIterator.NO_MORE_DOCS;
                }
                else {
                    _doc = _array[cursor];
                }
                return _doc;     
            }
        }

        @Override
        public int getCursor() {
            return cursor;
        }
    }

    public static <V> SVector<V> combine(SVector<V> first, SVector<V> rest, Combiner<V> combiner) {
        int firstLen = first.index.length;
        int restLen = rest.index.length;
        int[] newIndex = new int[firstLen + restLen];
        V[] newValue = (V[]) new Object[firstLen + restLen];

        StatefulDSIterator firstIndex = new ImmutableIntArrayDocIdSetIterator(first.index);
        StatefulDSIterator restIndex = new ImmutableIntArrayDocIdSetIterator(rest.index);

        try {
            int i = firstIndex.nextDoc();
            int j = restIndex.nextDoc();
            int k = 0;
            while (i != DocIdSetIterator.NO_MORE_DOCS && j != DocIdSetIterator.NO_MORE_DOCS) {
                if (i == j) {
                    V r = (V) combiner.combine((V)first.value[firstIndex.getCursor()], (V)rest.value[restIndex.getCursor()]);
                    if (r != null) {
                        newIndex[k] = i;
                        newValue[k] = r;
                        k++;
                    }
                    i = firstIndex.nextDoc();
                    j = restIndex.nextDoc();
                } else if (i < j) {
                    newIndex[k] = i;
                    newValue[k] = (V) first.value[firstIndex.getCursor()];
                    k++;
                    i = firstIndex.nextDoc();
                } else {
                    newIndex[k] = j;
                    newValue[k] = (V) rest.value[restIndex.getCursor()];
                    k++;
                    j = restIndex.nextDoc();

                }

            }

            if (i == DocIdSetIterator.NO_MORE_DOCS) {
                while (j != DocIdSetIterator.NO_MORE_DOCS) {
                    newIndex[k] = j;
                    newValue[k] = (V) rest.value[restIndex.getCursor()];
                    j = restIndex.nextDoc();
                    k++;
                }                
            } else {
                while (i != DocIdSetIterator.NO_MORE_DOCS) {
                    newIndex[k] = i;
                    newValue[k] = (V) first.value[firstIndex.getCursor()];
                    i = firstIndex.nextDoc();
                    k++;
                }                                
            }
            return new SVector<V>(IntArrays.trim(newIndex, k), ObjectArrays.trim(newValue, k));


        } catch (IOException e) {
            throw new RuntimeException(e);
        }



    }


    protected V[] values() {
        return (V[]) value;
    }

    protected int[] indexes() {
        return index;
    }

    public <X> SVector<X> transform(ITransformer<V,X> iTransformer, boolean nullIfEmpty) {
        int len = index.length;
        int[] newIndex = new int[len];
        X[] newValue = (X[]) new Object[len];


        int newI = 0;
        for (int i = 0; i < len; i++) {
            X newVal = (X) iTransformer.transform(index[i], (V)value[i]);
            if (newVal != null) {
                newIndex[newI] = index[i];
                newValue[newI] = newVal;
                newI++;
            }
        }
        if (newI == 0 && nullIfEmpty) return null;

        return new SVector<X>(IntArrays.trim(newIndex, newI),ObjectArrays.trim(newValue, newI));
    }


    public <X> SVector<X> transform(DocIdSet set, ITransformer<V,X> iTransformer, boolean nullIfEmpty) {
        try {
            int len = index.length;
            int[] newIndex = new int[len];
            X[] newValue = (X[]) new Object[len];
            int newI = 0;

            DocIdSetIterator matchIndex = set.iterator();
            StatefulDSIterator rowIndex = new ImmutableIntArrayDocIdSetIterator(index);

            int j = rowIndex.nextDoc();
            int i = matchIndex.advance(j);
            while (i != DocIdSetIterator.NO_MORE_DOCS && j != DocIdSetIterator.NO_MORE_DOCS) {
                if (i == j) {
                    int cursor = rowIndex.getCursor();
                    int doci = index[cursor];
                    X newVal = (X) iTransformer.transform(doci, (V)value[cursor]);
                    if (newVal != null) {
                        newIndex[newI] = doci;
                        newValue[newI] = newVal;
                        newI++;
                    }

                    j = rowIndex.nextDoc();
                    i = matchIndex.advance(j);
                } else if (i < j) {
                    i = matchIndex.advance(j);
                } else {
                    j = rowIndex.advance(i);
                }
            }

            if (newI == 0 && nullIfEmpty) return null;

            return new SVector<X>(IntArrays.trim(newIndex, newI),ObjectArrays.trim(newValue, newI));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }


    public int size() {
        return index.length;
    }

}
