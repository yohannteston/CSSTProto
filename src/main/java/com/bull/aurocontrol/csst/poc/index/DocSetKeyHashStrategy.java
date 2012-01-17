package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.Hash.Strategy;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.kamikaze.docidset.api.DocSet;

public final class DocSetKeyHashStrategy implements Strategy<DocSet> {
    public static final DocSetKeyHashStrategy INSTANCE = new DocSetKeyHashStrategy();

    @Override
    public int hashCode(DocSet o) {
        try {
            int result = 1;
            DocIdSetIterator it = o.iterator();
            for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                result = 31 * result + d;
            }
            
            return HashCommon.murmurHash3(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(DocSet a, DocSet b) {
        try {
            if (a.size() != b.size()) return false;
            DocIdSetIterator ait = a.iterator();
            DocIdSetIterator bit = b.iterator();
            
            for (int da = ait.nextDoc(), db = bit.nextDoc(); da != DocIdSetIterator.NO_MORE_DOCS && db != DocIdSetIterator.NO_MORE_DOCS; da = ait.nextDoc(), db = bit.nextDoc()) {
                if (da != db) return false;
            }
            
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}