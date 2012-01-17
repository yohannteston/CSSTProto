package com.bull.aurocontrol.csst.poc.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    
    public void testRealLucene() throws IOException {
        Monitor global = MonitorFactory.startPrimary("global");

        CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(new File("5AO.csv"), new File("PROFILE_WITHOUT_CIRCULAR.csv"));



        LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(4);


        Iterator<Flight> source = sourceFactory.iterate();

        final LuceneFlightSeason season =  (LuceneFlightSeason) factory.buildFlightSeason(source, 120);

        
        SMatrix<Integer> overlaps = season.queryOverlaps(4);
        
        //System.out.println(overlaps);
        
        Monitor phase = MonitorFactory.start("rule_check");
        
        final AnagramsRule anagramsRule = new AnagramsRule();
        final IdenticalFinalDigitsRule digitsRule = new IdenticalFinalDigitsRule();
        final ParallelCharactersRule parallelCharactersRule = new ParallelCharactersRule();
        
        final List<FlightPairData> conflicts = new ArrayList<FlightPairData>(100000);
        
        overlaps.transform(new IJTransformer() {

            @Override
            public Object transform(int i, int j, Object val) {
                Flight fi = season.getFlight(i);
                Flight fj = season.getFlight(j);
        
                Integer duration = (Integer) val;
                
                FlightPairData result = new FlightPairData(fi,fj,duration.intValue());
                
                result.setAnagram(anagramsRule.check(fi, fj));
                result.setIdenticalDigits(digitsRule.check(fi, fj));
                result.setParallelCharacters(parallelCharactersRule.check(fi, fj));
                
                if (result.isAnagram() || result.isIdenticalDigits() || result.isParallelCharacters()) {
                    conflicts.add(result);
                }
                
                return null;
            }
            
        });
        phase.stop();
        
        System.out.println(conflicts.size());
        

        global.stop();

        String[] header = MonitorFactory.getComposite("ms.").getDisplayHeader();
        Object[][] data = MonitorFactory.getComposite("ms.").getDisplayData();
        
        System.out.println(StringUtils.join(header,'\t'));
        for (int i = 0; i < data.length; i++) {
            System.out.println(StringUtils.join(data[i],'\t'));
            
        }
    }

    public static void main(String[] args) throws IOException {
        BasicTemporalIndexSeasonFactoryTest  test = new BasicTemporalIndexSeasonFactoryTest();

        test.testRealLucene();
    }

}
