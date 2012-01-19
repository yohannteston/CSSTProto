package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.collections.CollectionUtils;
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
import com.bull.aurocontrol.csst.poc.DayOfWeek;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSchedule;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.index.interval.NumericIntervalIntersectionQuery;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.utils.Combiner;
import com.bull.eurocontrol.csst.poc.utils.IClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.ITransformer;
import com.bull.eurocontrol.csst.poc.utils.ITransformerUtils;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.SVector;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.NotDocIdSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.kamikaze.docidset.utils.DocSetFactory.FOCUS;

public class LuceneIndexFlightSeason implements FlightSeason {

    private static final int MINUTE_PER_DAY = 60*24;
    private SMatrix<Integer> overlaps;
    private Flight[] db;
    private SearcherManager searcherManager;
    private int[] uids;
    private SMatrix<FlightPairData> conflictMatrix;
    private SVector<Integer> conflictCounters;
    private int buffer;

    public LuceneIndexFlightSeason(SMatrix<Integer> overlaps, Flight[] db, SearcherManager searcherManager, int[] uids, int buffer) {
        super();
        this.overlaps = overlaps;
        this.db = db;
        this.searcherManager = searcherManager;
        this.uids = uids;
        this.buffer = buffer;
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

                    ArrayList<short[]> conflictingSchedules = new ArrayList<short[]>();
                    
                    if (result.isAnagram() || result.isIdenticalDigits() || result.isParallelCharacters()) {
                        FlightSchedule[] fsi = fi.getSchedules();
                        FlightSchedule[] fsj = fj.getSchedules();
                        int conflictingSchedule = 0;
                        for (int k = 0; k < fsi.length; k++) {
                            FlightSchedule si = fsi[k];
                            for (int l = 0; l < fsj.length; l++) {
                                FlightSchedule sj = fsj[l];
                                
                                if (si.getFirstDayOfOperation() <= sj.getLastDayOfOperation() 
                                        && si.getLastDayOfOperation() >= sj.getFirstDayOfOperation()) {
                                    int eobt1 = fi.getEobt() - buffer;
                                    int eta1 = fi.getEta() + buffer;
                                    int eobt2 = fj.getEobt() - buffer;
                                    int eta2 = fj.getEta() + buffer;
                                    
                                    if (eobt1 <= eta2 && eta1 >= eobt2 
                                            && CollectionUtils.containsAny(si.getDaysOfOperation(), sj.getDaysOfOperation())) {
                                        conflictingSchedule++;
                                        conflictingSchedules.add(new short[] {(short) k,(short) l});
                                    } else if (eobt1 <= eta2 - MINUTE_PER_DAY && eta1 >= eobt2 - MINUTE_PER_DAY) { // 2 overlap if running previous day
                                        for (DayOfWeek r : sj.getDaysOfOperation()) {
                                            if (si.getDaysOfOperation().contains(r.next())) {
                                                conflictingSchedule++;
                                                conflictingSchedules.add(new short[] {(short) k,(short) l});
                                                break;
                                            }
                                        }
                                    }
                                    
                                }
                            }
                        }

                        
                        MutableInt c = counters.get(i);
                        if (c == null) {
                            counters.put(i, new MutableInt(conflictingSchedule));
                        } else {
                            c.add(conflictingSchedule);
                        }
                        
                        result.setConflictingSchedules(conflictingSchedules.toArray(new short[conflictingSchedules.size()][]));
                        
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
        private int maxValue = -1;
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
            int index = uids[base + doc];
            if (maxValue < index) maxValue = index;
            docIdSet.set(index);
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
        int maxIndex;
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
            maxIndex = results.maxValue;
        } finally {
            searcherManager.release(searcher);

            // Set to null to ensure we never again try to use
            // this searcher instance after releasing:
            searcher = null;
        }

        if (size == 0) return null;

        if (query.getMinimumConflicts() > 1) {
            flightsConcerned = filterByConflictCount(query, flightsConcerned, size, maxIndex);
        }

        if (size == 0) return null;

        final SMatrix<FlightPairData> conflictMatrix = filterConflicts(flightsConcerned, maxIndex);

        return conflictMatrix;
    }

    private DocIdSet filterByConflictCount(final ConflictQuery query, DocIdSet flightsConcerned, int size, int maxIndex) {
        final DocSet countFilteredFlightsConcerned = DocSetFactory.getDocSetInstance(0, maxIndex, size, FOCUS.PERFORMANCE);
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

    private SMatrix<FlightPairData> filterConflicts(final DocIdSet flightsConcerned, final int maxVal) {
        NotDocIdSet flightsNotConcerned = new NotDocIdSet(flightsConcerned, maxVal);
        final ITransformer<FlightPairData, FlightPairData> nopFP =ITransformerUtils.nop();
        
        final SMatrix<FlightPairData> result1 = conflictMatrix.transformEachRow(flightsNotConcerned,
                new ITransformer<SVector<FlightPairData>, SVector<FlightPairData>>() {

                    @Override
                    public SVector<FlightPairData> transform(final int i, SVector<FlightPairData> val) {                        
                        return val.transform(flightsConcerned, nopFP, true);
                    }
                });
        final ITransformer<SVector<FlightPairData>, SVector<FlightPairData>> nopFPV =ITransformerUtils.nop();
        
        final SMatrix<FlightPairData> result2 = conflictMatrix.transformEachRow(flightsConcerned,nopFPV);
        final SMatrix<FlightPairData> result = SMatrix.combine(result1, result2, new Combiner<FlightPairData>() {

            @Override
            public FlightPairData combine(FlightPairData a, FlightPairData b) {
                return (a != null) ? a : b;
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
