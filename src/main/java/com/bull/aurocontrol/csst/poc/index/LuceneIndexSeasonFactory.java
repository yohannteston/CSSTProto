package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

import com.bull.aurocontrol.csst.poc1.DayOfWeek;
import com.bull.aurocontrol.csst.poc1.Flight;
import com.bull.aurocontrol.csst.poc1.FlightSeason;
import com.bull.aurocontrol.csst.poc1.FlightSeasonFactory;

public class LuceneIndexSeasonFactory implements FlightSeasonFactory {

    private static final int MINUTE_PER_DAY = 60*24;
    private int periodDuration = 120;

    @Override
    public FlightSeason buildFlightSeason(Iterator<Flight> source, int bufferDuration) {
        StopWatch loading = new StopWatch();
        loading.start();

        long firstDayOfSeason = Long.MAX_VALUE;            
        long lastDayOfSeason = Long.MIN_VALUE;            

        List<Flight> db = new ArrayList<Flight>();
        while (source.hasNext()) {
            Flight flight = (Flight) source.next();
            db.add(flight);

            if (firstDayOfSeason > flight.getFirstDayOfOperation().getTime()) {
                firstDayOfSeason = flight.getFirstDayOfOperation().getTime();
            }
            if (lastDayOfSeason < flight.getLastDayOfOperation().getTime()) {
                lastDayOfSeason = flight.getLastDayOfOperation().getTime();
            }

        }            
        loading.stop();       

        HashMap<String, LocationOccupation> locations = new HashMap<String, LocationOccupation>();

        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT));

        config.setOpenMode(OpenMode.CREATE);
        config.setRAMBufferSizeMB(1024);
        
        try {
            IndexWriter writer = new IndexWriter(dir, config);

            for (int i = 0, endi = db.size(); i < endi; i++) {
                Flight flight = db.get(i);
                Document doc = new Document();



                int firstDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(flight.getFirstDayOfOperation().getTime() - firstDayOfSeason);
                int lastDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(flight.getLastDayOfOperation().getTime() - firstDayOfSeason);


                DayOfWeek dow = DayOfWeek.get(flight.getFirstDayOfOperation());

                EnumSet<DayOfWeek> dowOfOperation = flight.getDaysOfOperation();

                for (int dayOfSeason = firstDayOfOpInSeason; dayOfSeason <= lastDayOfOpInSeason; dayOfSeason++, dow = dow.next()) {
                    if (dowOfOperation.contains(dow)) {
                        int start = (dayOfSeason * MINUTE_PER_DAY + flight.getEobt() - bufferDuration) ;
                        int end = (dayOfSeason * MINUTE_PER_DAY + flight.getEobt() + bufferDuration) ;

                        addLocation(doc, flight.getDepartureAirport(), start, end);


                        start = (dayOfSeason * MINUTE_PER_DAY + flight.getEta() - bufferDuration) ;
                        end = (dayOfSeason * MINUTE_PER_DAY + flight.getEta() + bufferDuration) ;

                        addLocation(doc, flight.getDestinationAirport(), start, end);

                        for (int j = 0, endj = flight.countAirspace(); j < endj; j++) {
                            start = (dayOfSeason * MINUTE_PER_DAY + flight.getStart(j) - bufferDuration) ;
                            end = (dayOfSeason * MINUTE_PER_DAY + flight.getEnd(j) + bufferDuration) ;

                            addLocation(doc, flight.getAirspace(j), start, end);
                        }
                    }                
                }

                writer.addDocument(doc);


            }
            
            writer.commit();
            writer.optimize();
        
            writer.close();
            
            return new LuceneFlightSeason(IndexReader.open(dir));

        } catch (CorruptIndexException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private void addLocation(Document doc, String location, int start, int end) {
        int firstPeriod = start / periodDuration ;
        int lastPeriod = end / periodDuration + 1;

        doc.add(new Field(location +"_S",NumericUtils.intToPrefixCoded(firstPeriod), Store.NO,Index.ANALYZED_NO_NORMS));
        doc.add(new Field(location +"_E",NumericUtils.intToPrefixCoded(lastPeriod), Store.NO,Index.ANALYZED_NO_NORMS));

        for (int k = firstPeriod+1; k < lastPeriod; k++) {
            doc.add(new Field(location,NumericUtils.intToPrefixCoded(k), Store.NO,Index.ANALYZED_NO_NORMS));
        }
    }

  
}
