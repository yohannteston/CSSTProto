package com.bull.aurocontrol.csst.poc.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import jsr166y.ForkJoinPool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bull.aurocontrol.csst.poc.ConflictQuery;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.source.CSVFlightSourceFactory;
import com.bull.eurocontrol.csst.poc.utils.Combiner;
import com.bull.eurocontrol.csst.poc.utils.IJClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.JamonUtils;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class BasicTemporalIndexSeasonFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPeriods() throws IOException {

        ForkJoinPool pool = new ForkJoinPool(1);
        CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(new File("Results/test-flights.csv"), new File("Results/catalog.csv"));

        LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(1, false);

        Iterator<Flight> source = sourceFactory.iterate();

        final LuceneIndexFlightSeason season =  (LuceneIndexFlightSeason) factory.buildFlightSeason(source, 25, pool);

        
        SMatrix<Integer> overlaps1 = season.getOverlapMatrix();
        
        factory = new LuceneIndexSeasonFactory(2,false);

        source = sourceFactory.iterate();

        final LuceneIndexFlightSeason season2 =  (LuceneIndexFlightSeason) factory.buildFlightSeason(source, 25, pool);

        
        SMatrix<Integer> overlaps2 = season2.getOverlapMatrix();
        
        SMatrix<Integer> deltas = SMatrix.combine(overlaps1, overlaps2, new Combiner<Integer>() {

            @Override
            public Integer combine(Integer a, Integer b) {
                if (a.equals(b)) {
                    return null;
                }
                return a - b;
            }
            
        });
        deltas.execute(new IJClosure<Integer>() {

            @Override
            public void execute(int i, int j, Integer input) {
               System.out.println(season.getFlight(i));
               System.out.println(season.getFlight(j));
               System.out.println(i);
               System.out.println(j);
               System.exit(-1);
            }
            
        });
        
        System.out.println(deltas);
        System.out.println(deltas.size());
    }

    @Test
    public void testQuery() throws IOException {
        ForkJoinPool pool = new ForkJoinPool(1);
        CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(new File("Results/test-flights.csv"), new File("Results/catalog.csv"));

        LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(1, true);

        Iterator<Flight> source = sourceFactory.iterate();

        final LuceneIndexFlightSeason season =  (LuceneIndexFlightSeason) factory.buildFlightSeason(source, 25, pool);

        testQuery(season, new ConflictQuery(null, null, null, null, 0));
        testQuery(season, new ConflictQuery(null, null, null, null, 2));
        testQuery(season, new ConflictQuery("LEMD", null, null, null, 0));
        testQuery(season, new ConflictQuery("LEMD", null, new Date(1301184000000L), new Date(1306627200000L), 0));
        testQuery(season, new ConflictQuery("LEMD", null, new Date(1301184000000L), new Date(1306627200000L), 2));
        
        JamonUtils.outputJamonReport(null);
        
    }

    private void testQuery(final LuceneIndexFlightSeason season, ConflictQuery q) throws IOException {
        Monitor mon = MonitorFactory.start("query");
        
        SMatrix<FlightPairData> result = season.queryConflicts(q);
        System.out.println(result.size());
        
        mon.stop();
    }

}
