package com.bull.aurocontrol.csst.poc.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jsr166y.ForkJoinPool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(1);

        Iterator<Flight> source = sourceFactory.iterate();

        final LuceneFlightSeason season =  (LuceneFlightSeason) factory.buildFlightSeason(source, 25, pool);

        
        SMatrix<Integer> overlaps1 = season.queryOverlaps(1);
        
        factory = new LuceneIndexSeasonFactory(2);

        source = sourceFactory.iterate();

        final LuceneFlightSeason season2 =  (LuceneFlightSeason) factory.buildFlightSeason(source, 25, pool);

        
        SMatrix<Integer> overlaps2 = season2.queryOverlaps(1);
        
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


}
