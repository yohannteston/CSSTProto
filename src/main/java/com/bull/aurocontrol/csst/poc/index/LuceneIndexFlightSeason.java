package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.io.IOException;

import org.apache.commons.collections.FactoryUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;

import com.bull.aurocontrol.csst.poc.ConflictQuery;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.index.interval.NumericIntervalIntersectionQuery;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.utils.IClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.ITransformer;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.SVector;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.kamikaze.docidset.utils.DocSetFactory.FOCUS;

public class LuceneIndexFlightSeason implements FlightSeason {

    private SMatrix<Integer> overlaps;
    private Flight[] db;
    private SearcherManager searcherManager;
    private int[] uids;
    private SMatrix<FlightPairData> conflictMatrix;
    private SVector<Integer> conflictCounters;

    public LuceneIndexFlightSeason(SMatrix<Integer> overlaps, Flight[] db, SearcherManager searcherManager, int[] uids) {
        super();
        this.overlaps = overlaps;
        this.db = db;
        this.searcherManager = searcherManager;
        this.uids = uids;
    }

    @Override
    public SMatrix<FlightPairData> queryConflicts() throws IOException {
        buildConflictMatrix();

        return conflictMatrix;

    }

    protected void buildConflictMatrix() {
        if (conflictMatrix == null) {
            final Int2ObjectAVLTreeMap<MutableInt> counters = new Int2ObjectAVLTreeMap<MutableInt>();
            final AnagramsRule anagramsRule = new AnagramsRule();
            final IdenticalFinalDigitsRule digitsRule = new IdenticalFinalDigitsRule();
            final ParallelCharactersRule parallelCharactersRule = new ParallelCharactersRule();
            conflictMatrix = overlaps.transform(new IJTransformer<Integer, FlightPairData>() {

                @Override
                public FlightPairData transform(int i, int j, Integer val) {
                    Flight fi = getFlight(i);
                    Flight fj = getFlight(j);

                    Integer duration = (Integer) val;

                    FlightPairData result = new FlightPairData(fi, fj, duration.intValue());

                    result.setAnagram(!anagramsRule.check(fi, fj));
                    result.setIdenticalDigits(!digitsRule.check(fi, fj));
                    result.setParallelCharacters(!parallelCharactersRule.check(fi, fj));

                    if (result.isAnagram() || result.isIdenticalDigits() || result.isParallelCharacters()) {
                        MutableInt c = counters.get(i);
                        if (c == null) {
                            counters.put(i, new MutableInt(1));
                        } else {
                            c.increment();
                        }
                        return result;
                    } else {
                        return null;
                    }
                }

            });
            conflictCounters = new SVector<Integer>(counters, new Transformer() {

                @Override
                public Object transform(Object input) {
                    return Integer.valueOf(((MutableInt) input).intValue());
                }

            });
        }
    }

    @Override
    public Flight getFlight(int i) {
        return db[i];
    }

    public class DocSetCollector extends Collector {
        private int size = 0;
        private final OpenBitSet docIdSet;

        private int base;

        public DocSetCollector(IndexReader reader) {
            this.docIdSet = new OpenBitSet(reader.maxDoc());
        }

        public DocIdSet docIdSet() {
            return docIdSet;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public void collect(int doc) throws IOException {
            docIdSet.set(uids[base + doc]);
            size++;
        }

        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
            base = docBase;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }

        public int getSize() {
            return size;
        }
    }

    @Override
    public SMatrix<FlightPairData> queryConflicts(final ConflictQuery query) throws IOException {
        DocIdSet flightsConcerned;
        int size = 0;
        buildConflictMatrix();

        IndexSearcher searcher = searcherManager.acquire();
        try {
            DocSetCollector results = new DocSetCollector(searcher.getIndexReader());
            BooleanQuery bmq = buildQuery(query);

            if (bmq.clauses().size() == 0) {
                if (query.getMinimumConflicts() <= 1) {
                    return conflictMatrix;
                }
                searcher.search(new MatchAllDocsQuery(), results);
            } else {
                searcher.search(bmq, results);
            }

            flightsConcerned = results.docIdSet();
            size = results.getSize();
        } finally {
            searcherManager.release(searcher);

            // Set to null to ensure we never again try to use
            // this searcher instance after releasing:
            searcher = null;
        }

        if (size == 0) return null;

        if (query.getMinimumConflicts() > 1) {
            flightsConcerned = filterByConflictCount(query, flightsConcerned, size);
        }

        if (size == 0) return null;

        final SMatrix<FlightPairData> conflictMatrix = filterConflicts(flightsConcerned);

        return conflictMatrix;
    }

    private DocIdSet filterByConflictCount(final ConflictQuery query, DocIdSet flightsConcerned, int size) {
        final DocSet countFilteredFlightsConcerned = DocSetFactory.getDocSetInstance(-1, -1, size, FOCUS.PERFORMANCE);
        conflictCounters.executeOnEachItem(flightsConcerned, new IClosure<Integer>() {

            @Override
            public void execute(int i, Integer input) {
                if (input >= query.getMinimumConflicts()) {
                    try {
                        countFilteredFlightsConcerned.addDoc(i);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        });

        flightsConcerned = countFilteredFlightsConcerned;
        return flightsConcerned;
    }

    private SMatrix<FlightPairData> filterConflicts(final DocIdSet flightsConcerned) {
        final SMatrix<FlightPairData> result = conflictMatrix.transformEachRow(flightsConcerned,
                new ITransformer<SVector<FlightPairData>, SVector<FlightPairData>>() {

                    @Override
                    public SVector<FlightPairData> transform(final int i, SVector<FlightPairData> val) {
                        return val.transform(flightsConcerned, new ITransformer<FlightPairData, FlightPairData>() {

                            @Override
                            public FlightPairData transform(final int j, FlightPairData fpd) {
                                return fpd;
                            }
                        }, true);
                    }
                });
        return result;
    }

    private BooleanQuery buildQuery(ConflictQuery query) {
        BooleanQuery bmq = new BooleanQuery(true);
        if (query.getAdep() != null) {
            bmq.add(new TermQuery(new Term(LuceneIndexSeasonFactory.META_FLD_ADEP, query.getAdep())), Occur.MUST);
        }
        if (query.getAdes() != null) {
            bmq.add(new TermQuery(new Term(LuceneIndexSeasonFactory.META_FLD_ADEP, query.getAdep())), Occur.MUST);
        }
        long from = 0, to = 0;

        if (query.getFrom() != null) {
            from = query.getFrom().getTime();
            if (query.getTo() != null) {
                to = query.getTo().getTime();
            } else {
                to = Long.MAX_VALUE;
            }
        } else if (query.getTo() != null) {
            from = 0;
            to = query.getTo().getTime();
        }
        if (to != 0) {
            bmq.add(new NumericIntervalIntersectionQuery(LuceneIndexSeasonFactory.META_FLD_DOP, from, to), Occur.MUST);
        }
        return bmq;
    }

    public SMatrix<Integer> getOverlapMatrix() {
        return overlaps;
    }

}
