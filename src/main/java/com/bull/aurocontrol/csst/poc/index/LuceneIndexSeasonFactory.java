package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

import com.bull.aurocontrol.csst.poc.AirspaceProfile;
import com.bull.aurocontrol.csst.poc.DayOfWeek;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightProfile;
import com.bull.aurocontrol.csst.poc.FlightSchedule;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.FlightSeasonFactory;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class LuceneIndexSeasonFactory implements FlightSeasonFactory {

    public static final int MINUTE_PER_DAY = 60 * 24;
    public static final int MINUTE_PER_WEEK = MINUTE_PER_DAY * 7;
    private int periodDuration;
    private int periodPerWeek;

    

    
    public LuceneIndexSeasonFactory(int periodPerDay) {
        super();
        periodDuration = MINUTE_PER_DAY / periodPerDay;
        periodPerWeek = MINUTE_PER_WEEK / periodDuration;
    }


    @Override
    public FlightSeason buildFlightSeason(Iterator<Flight> source, int bufferDuration) {
        Monitor mon = MonitorFactory.start("index");
        Monitor phase = MonitorFactory.start("index/loading");
        
        long firstDayOfSeason = Long.MAX_VALUE;
        long lastDayOfSeason = Long.MIN_VALUE;

        HashSet<String> locations = new HashSet<String>(); 

        List<Flight> db = new ArrayList<Flight>();
        while (source.hasNext()) {
            Flight flight = (Flight) source.next();
            db.add(flight);

            locations.add(flight.getDepartureAirport());
            locations.add(flight.getDestinationAirport());
            for (int i = 0; i < flight.countAirspace(); i++) {
                locations.add(flight.getAirspace(i).getName());
            }


            long fFirstDayOfOperation = flight.getFirstDayOfOperation();
            if (firstDayOfSeason > fFirstDayOfOperation) {
                firstDayOfSeason = fFirstDayOfOperation;
            }
            
            final long fLastDayOfOperation = flight.getLastDayOfOperation();
            if (lastDayOfSeason < fLastDayOfOperation) {
                lastDayOfSeason = fLastDayOfOperation;
            }

        }
        phase.stop();        
        phase = MonitorFactory.start("index/lucene");

        int totalSchedules = 0;

        DayOfWeek dowOfFirstDayOfSeason = DayOfWeek.get(firstDayOfSeason);
        
        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
        config.setRAMBufferSizeMB(256);
        try {   
            IndexWriter writer = new IndexWriter(dir,config);
            for (int i = 0, endi = db.size(); i < endi; i++) {
                Flight flight = db.get(i);
                
                FlightSchedule[] schedules = flight.getSchedules();
                final int numSched = schedules.length;

                totalSchedules += numSched;
                
                Document doc = new Document();
                doc.add(new UIDField("_UID_", i));
                int eobt = flight.getEobt();
                int eta = flight.getEta();
                
                
                int numAirspace = flight.countAirspace();
                int[] airspaceStart = new int[numAirspace];
                int[] airspaceEnd = new int[numAirspace]; 
                
                if (numAirspace > 0) {
                    FlightProfile profile = flight.getProfile();
                    int etot = eobt + profile.getTaxitime();
                    int duration = eta - eobt;

                    if (profile.getDuration() == 0) {
                        if (numAirspace == 1 && flight.getAirspace(0).getStart() == 0) {
                            airspaceStart[0] = eobt;
                            airspaceEnd[0] = eta;
                        } else {
                            throw new RuntimeException("invalid catalog ?");
                        }
                    } else {
                        for (int k = 0; k < numAirspace; k++) {
                            AirspaceProfile airspace = flight.getAirspace(k);
    
                            airspaceStart[k] = etot + airspace.getStart() * duration / profile.getDuration();
                            airspaceEnd[k] = etot + airspace.getEnd() * duration / profile.getDuration();
                        }                    
                    }
                }

                // calc entering and leaving from profile
//                if (flight.getCfn().equals("1014") || flight.getCfn().equals("1144")) {
//                    System.out.println(flight);
//                    System.out.println(Arrays.toString(flight.getAirspaces()));
//                    System.out.println(Arrays.toString(airspaceStart));
//                    System.out.println(Arrays.toString(airspaceEnd));
//                }

                
                // add values for the week index
                EnumSet<DayOfWeek> dowIndexed = EnumSet.noneOf(DayOfWeek.class);
                for (int schedI = 0; schedI < numSched; schedI++) {
                    FlightSchedule sched = schedules[schedI];
                    dowIndexed.addAll(sched.getDaysOfOperation());
                }
                for (DayOfWeek dayOfWeek : dowIndexed) {
                    int dayOrd = (dayOfWeek.ordinal() - dowOfFirstDayOfSeason.ordinal() + 7) % 7; 
                    int start = (dayOrd * MINUTE_PER_DAY + eobt - bufferDuration);
                    int end = (dayOrd * MINUTE_PER_DAY + eobt + bufferDuration);

                    addToWeekIndex(doc, flight.getDepartureAirport(), start, end);

                    start = (dayOrd * MINUTE_PER_DAY + eta - bufferDuration);
                    end = (dayOrd * MINUTE_PER_DAY + eta + bufferDuration);

                    addToWeekIndex(doc, flight.getDestinationAirport(), start, end);


                    for (int k = 0; k < numAirspace; k++) {
                        AirspaceProfile airspace = flight.getAirspace(k);

                        start = (dayOrd * MINUTE_PER_DAY + airspaceStart[k] - bufferDuration);
                        end = (dayOrd * MINUTE_PER_DAY + airspaceEnd[k] + bufferDuration);

                        addToWeekIndex(doc, airspace.getName(), start, end);
                    }

                    dowIndexed.add(dayOfWeek);
                }

                
                // add to season index
                for (int schedI = 0; schedI < numSched; schedI++) {
                    FlightSchedule sched = schedules[schedI];
                    EnumSet<DayOfWeek> dowOfOperation = sched.getDaysOfOperation();
                    
                    
                    
                    // add values to season index
                    DayOfWeek dow = DayOfWeek.get(sched.getFirstDayOfOperation());
                    int firstDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(sched.getFirstDayOfOperation() - firstDayOfSeason);
                    int lastDayOfOpInSeason = (int) TimeUnit.MILLISECONDS.toDays(sched.getLastDayOfOperation() - firstDayOfSeason);

//                    if (flight.getCfn().equals("006") || flight.getCfn().equals("3206")) {
//                        System.out.println(dow);
//                        System.out.println(DayOfWeek.get(sched.getFirstDayOfOperation()));
//                        System.out.println(new java.util.Date(sched.getFirstDayOfOperation()));
//                        System.out.println(new java.util.Date(sched.getLastDayOfOperation()));
//                        System.out.println(firstDayOfOpInSeason);
//                        System.out.println(lastDayOfOpInSeason);
//                    }
                    
                    for (int dayOfSeason = firstDayOfOpInSeason; dayOfSeason <= lastDayOfOpInSeason; dayOfSeason++, dow = dow.next()) {
                        if (dowOfOperation.contains(dow)) {
                            int start = (dayOfSeason * MINUTE_PER_DAY + eobt - bufferDuration);
                            int end = (dayOfSeason * MINUTE_PER_DAY + eobt + bufferDuration);

                            addLocation(doc, flight.getDepartureAirport(), start, end);

                            start = (dayOfSeason * MINUTE_PER_DAY + eta - bufferDuration);
                            end = (dayOfSeason * MINUTE_PER_DAY + eta + bufferDuration);

                            addLocation(doc, flight.getDestinationAirport(), start, end);

                            for (int k = 0; k < numAirspace; k++) {
                                AirspaceProfile airspace = flight.getAirspace(k);

                                start = (dayOfSeason * MINUTE_PER_DAY + airspaceStart[k] - bufferDuration);
                                end = (dayOfSeason * MINUTE_PER_DAY + airspaceEnd[k] + bufferDuration);

                                addLocation(doc, airspace.getName(), start, end);
                            }
                        }
                    }

                }
                
                    
                    //if (flight.getCfn().equals("003") || flight.getCfn().equals("2203"))

                writer.addDocument(doc);
            }

            writer.commit();
            writer.close();

            IndexReader reader = IndexReader.open(dir);
            int[] uids = UIDField.load(reader, "_UID_");
            
            phase.stop();
            mon.stop();        
            
            System.out.printf("number of flights : %s\n", db.size());
            System.out.printf("number of schedules : %s\n", totalSchedules);
            System.out.printf("number of doc in index : %s\n", reader.numDocs());
            

//            Directory to = new NIOFSDirectory(new File("backup"));
//            for (String file : dir.listAll()) {
//                dir.copy(to, file, file); 
//            }
//            to.close();

            return new LuceneFlightSeason(reader, locations, periodDuration, uids, db.toArray(new Flight[db.size()]));

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

  



    private void sysoutSize(Directory dir) throws IOException {
        long size = 0;
        for (String file : dir.listAll()) {
            size += dir.fileLength(file);
        }

        System.out.println(size);
    }
    
    private void addToWeekIndex(Document doc, String location, int start, int end) {
        int startInFirstPeriod = start % periodDuration;
        int endInLastPeriod = end % periodDuration;

        int firstPeriod = (start / periodDuration) % periodPerWeek;
        int lastPeriod = (end / periodDuration) % periodPerWeek;

        Field field = new Field(weekFieldName(location, firstPeriod), weekFieldValue(startInFirstPeriod, 'a'), Store.NO,Index.ANALYZED_NO_NORMS);
        field.setIndexOptions(IndexOptions.DOCS_ONLY);

        doc.add(field);

        field = new Field(weekFieldName(location, lastPeriod), weekFieldValue(endInLastPeriod, 'r'), Store.NO,Index.ANALYZED_NO_NORMS);
        field.setIndexOptions(IndexOptions.DOCS_ONLY);

        doc.add(field);
        
        if (firstPeriod != lastPeriod) {
            for (int k = firstPeriod + 1; k <= lastPeriod; k++) {
                field = new Field(weekFieldName(location, k), "0", Store.NO,Index.ANALYZED_NO_NORMS);
                field.setIndexOptions(IndexOptions.DOCS_ONLY);
                
                doc.add(field);
            }
        }
    }

    private String weekFieldValue(int timestamp, char type) {
        StringBuilder b = new StringBuilder(20);
        b.append(NumericUtils.intToPrefixCoded(timestamp));
        b.append('_');
        b.append(type);
        return b.toString();
    }

    private String weekFieldName(String location, int p) {
        StringBuilder b = new StringBuilder(20);
        b.append("week_");
        b.append(location);
        b.append('_');
        b.append(NumericUtils.intToPrefixCoded(p));
        return b.toString();
    }

    private void addLocation(Document doc, String location, int start, int end) {
        final int firstPeriod = start / periodDuration;
        final int lastPeriod = end / periodDuration;
        final String seasonFieldName = location;
        
        if (firstPeriod == lastPeriod) {
            Field field = new Field(seasonFieldName, false, NumericUtils.intToPrefixCoded(firstPeriod), Store.NO, Index.ANALYZED_NO_NORMS, TermVector.NO); 
            field.setIndexOptions(IndexOptions.DOCS_ONLY);
            doc.add(field);
        } else {
            for (int k = firstPeriod; k <= lastPeriod; k++) {
                Field field = new Field(seasonFieldName, false, NumericUtils.intToPrefixCoded(k), Store.NO,Index.ANALYZED_NO_NORMS, TermVector.NO); 
                field.setIndexOptions(IndexOptions.DOCS_ONLY);
                doc.add(field);
            }
        }
    }

  

}
