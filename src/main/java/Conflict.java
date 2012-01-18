
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.time.FastDateFormat;

import com.bull.aurocontrol.csst.poc.DayOfWeek;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSchedule;
import com.bull.aurocontrol.csst.poc.index.LuceneFlightSeason;
import com.bull.aurocontrol.csst.poc.index.LuceneIndexSeasonFactory;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.source.CSVFlightSourceFactory;
import com.bull.eurocontrol.csst.poc.utils.IJClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.MemoryWatch;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class Conflict {

    public static final DecimalFormat DIGIT2_FORMAT = new DecimalFormat("00");
    private static final FastDateFormat OUT_DATE_FORMAT = FastDateFormat.getInstance("yyMMdd");
    private static final int MINUTE_PER_DAY = 24*60;

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
     // create Options object
        Options options = new Options();

        // test options
        options.addOption("r", "repeat", true, "number of repetition (default: 1 => none)");
        // tech options
        options.addOption("t", "thread", true, "the paralelism level (default: 1 => none)");
        options.addOption("p", "periods", true, "periods in the index per day (default: 1)");
        // biz option
        options.addOption("b", "buffer", true, "buffer time for overlap calcs (default: 0)");
        options.addOption("c", "conflict_output", true, "output conflicts csv file");
        options.addOption("j", "performance", true, "output performance counters csv file");
        
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse( options, args);
            
            if (cmd.getArgs().length == 0) {
                System.out.println("ERROR: input files required");
                printHelp(options);
                System.exit(-1);
            }
            File input = new File(cmd.getArgs()[0]);
            File catalog = new File(cmd.getArgs()[1]);
            
            int repeat = NumberUtils.toInt(cmd.getOptionValue("r", "1"));
            int threads = NumberUtils.toInt(cmd.getOptionValue("t", "1"));
            int periods = NumberUtils.toInt(cmd.getOptionValue("p", "1"));
            final int buffer = NumberUtils.toInt(cmd.getOptionValue("b", "0")) / 2;
            
            File conflicts = (cmd.hasOption("c")) ? new File(cmd.getOptionValue("c")) : null;
            File jamon = (cmd.hasOption("j")) ? new File(cmd.getOptionValue("j")) : null;
            
            System.out.println("Warming up: the process is done once to emulate normal server JIT optimization.");
            doTest(input, catalog, conflicts, threads, periods, buffer);
            MemoryWatch.gc();
            MonitorFactory.reset();
            System.out.printf("Now reperating the process %s times and take the performance measures. Each time ensure gc()\n", repeat);
            for (int i = 0; i < repeat; i++) {
                doTest(input, catalog, conflicts, threads, periods, buffer);                
                MemoryWatch.gc();
            }
            

            String[] header = MonitorFactory.getComposite("ms.").getDisplayHeader();
            Object[][] data = MonitorFactory.getComposite("ms.").getDisplayData();
            
            Arrays.sort(data, new Comparator<Object[]>() {

                @Override
                public int compare(Object[] o1, Object[] o2) {
                    return ((String)o1[0]).compareTo((String)o2[0]);
                }
                
            });
            
            BufferedWriter jamonWriter;
            if (jamon != null) {
                jamonWriter = new BufferedWriter(new FileWriter(jamon,false));
            } else {
                jamonWriter = new BufferedWriter(new OutputStreamWriter(System.out));
            }

            jamonWriter.write(StringUtils.join(header,'\t'));
            jamonWriter.newLine();
            for (int i = 0; i < data.length; i++) {
                jamonWriter.write(StringUtils.join(data[i],'\t'));
                jamonWriter.newLine();
            }
            jamonWriter.flush();
            
//            System.out.println("comparing");
//            
//            FileWriter report  = new FileWriter("errors.txt");
//            
//            
//            BufferedReader reader = new BufferedReader(new FileReader(new File("Results/ada-conflicts.csv")));
//            Map<String,String> adaConflicts = new HashMap<String, String>();
//            fillConflict(reader, adaConflicts);
//            reader.close();
//
//             reader = new BufferedReader(new FileReader(new File("Results/java-conflicts.csv")));
//            Map<String,String> javaConflicts = new HashMap<String, String>();
//            fillConflict(reader, javaConflicts);
//            reader.close();
//            
//            javaConflicts.keySet().removeAll(adaConflicts.keySet());
//            int x =0;
//            
//            report.append("********* JAVA ONLY *************\n");
//            for (String line : new TreeSet<String>(javaConflicts.values())) {
//                report.append(++x + " " +line + "\n");
//            }
//           
//            reader = new BufferedReader(new FileReader(new File("Results/java-conflicts.csv")));
//            javaConflicts = new HashMap<String, String>();
//           fillConflict(reader, javaConflicts);
//           reader.close();
//           adaConflicts.keySet().removeAll(javaConflicts.keySet());
//            
//           report.append("********* ADA ONLY *************\n");
//            x = 0;
//            for (String line : new TreeSet<String>(adaConflicts.values())) {
//                report.append(++x + " " +line + "\n");
//            }
//            report.close();
            jamonWriter.close();

        } catch (Exception e) {
            printHelp(options);
            throw e;
        }
    }

    private static void doTest(File input, File catalog, File conflicts, int threads, int periods, final int buffer) throws IOException {
        Monitor global = MonitorFactory.startPrimary("_TOTAL_");

        CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(input, catalog);

        LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(periods);

        Iterator<Flight> source = sourceFactory.iterate();

        final LuceneFlightSeason season =  (LuceneFlightSeason) factory.buildFlightSeason(source, buffer);
         
        SMatrix<Integer> overlapMatrix = season.queryOverlaps(threads);
        //System.out.println(overlapMatrix);
        System.out.println("overlaps:" + overlapMatrix.size());
        
        
        Monitor phase = MonitorFactory.start("rule_check");
        
        final AnagramsRule anagramsRule = new AnagramsRule();
        final IdenticalFinalDigitsRule digitsRule = new IdenticalFinalDigitsRule();
        final ParallelCharactersRule parallelCharactersRule = new ParallelCharactersRule();
                    
        
        
        SMatrix<FlightPairData> conflictMatrix = overlapMatrix.transform(new IJTransformer<Integer,FlightPairData>() {

            @Override
            public FlightPairData transform(int i, int j, Integer val) {
                Flight fi = season.getFlight(i);
                Flight fj = season.getFlight(j);

                Integer duration = (Integer) val;
                
                FlightPairData result = new FlightPairData(fi,fj,duration.intValue());
                
                result.setAnagram(!anagramsRule.check(fi, fj));
                result.setIdenticalDigits(!digitsRule.check(fi, fj));
                result.setParallelCharacters(!parallelCharactersRule.check(fi, fj));
                
                if (result.isAnagram() || result.isIdenticalDigits() || result.isParallelCharacters()) {
                    return result;
                } else {
                    return null;
                }
            }
            
        });
        
        System.out.println("conflicting flights index entries:" + conflictMatrix.size());
        
        phase.stop();
        phase = MonitorFactory.start("rule_conflict schedules output");
        
        final MutableInt conflictingSchedule = new MutableInt();
        final BufferedWriter conflictsWriter;
        
        
        
        if (conflicts != null) {
            conflictsWriter = new BufferedWriter(new FileWriter(conflicts,false));
        } else {
            conflictsWriter = null;
        }
        
        conflictMatrix.execute(new IJClosure<FlightPairData>() {

            @Override
            public void execute(int i, int j, FlightPairData input) {
                Flight fi = season.getFlight(i);
                Flight fj = season.getFlight(j);
                FlightSchedule[] fsi = fi.getSchedules();
                FlightSchedule[] fsj = fj.getSchedules();
                
                for (FlightSchedule si : fsi) {
                    for (FlightSchedule sj : fsj) {
                        if (si.getFirstDayOfOperation() <= sj.getLastDayOfOperation() 
                                && si.getLastDayOfOperation() >= sj.getFirstDayOfOperation()) {
                            int eobt1 = fi.getEobt() - buffer;
                            int eta1 = fi.getEta() + buffer;
                            int eobt2 = fj.getEobt() - buffer;
                            int eta2 = fj.getEta() + buffer;
                            
                            if (eobt1 <= eta2 && eta1 >= eobt2 
                                    && CollectionUtils.containsAny(si.getDaysOfOperation(), sj.getDaysOfOperation())) {
                                conflictingSchedule.increment();
                                printConflict(input, fi, fj, si, sj, conflictsWriter);
                            } else if (eobt1 <= eta2 - MINUTE_PER_DAY && eta1 >= eobt2 - MINUTE_PER_DAY) { // 2 overlap if running previous day
                                for (DayOfWeek r : sj.getDaysOfOperation()) {
                                    if (si.getDaysOfOperation().contains(r.next())) {
                                        conflictingSchedule.increment();
                                        printConflict(input, fi, fj, si, sj, conflictsWriter);
                                        break;
                                    }
                                }
                            }
                            
                        }
                    }
                }
            }

            private void printConflict(FlightPairData input, Flight f1, Flight f2, FlightSchedule si, FlightSchedule sj, BufferedWriter conflictsWriter) {
                if (conflictsWriter == null) return;
                try {
                    printFlightNSchedule(f1, si, conflictsWriter);
                    conflictsWriter.append(',');
                    printFlightNSchedule(f2, sj, conflictsWriter);
                    conflictsWriter.append(',');
                    if (input.isAnagram()) {
                        conflictsWriter.append("ANAGRAM");
                    } else if (input.isIdenticalDigits()) {
                        conflictsWriter.append("LAST_FINAL_2_CHARS_AJACCIO");
                    } else if (input.isParallelCharacters()) {
                        conflictsWriter.append("PARALLEL_CHARACTERS");                            
                    } else {
                        throw new RuntimeException("bug");
                    }
//                        conflictsWriter.append(',');
//                        conflictsWriter.append(f1.toString());
//                        conflictsWriter.append(',');
//                        conflictsWriter.append(f2.toString());                        
                    conflictsWriter.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                
            }

            private void printFlightNSchedule(Flight f1, FlightSchedule si, BufferedWriter conflictsWriter) throws IOException {
                conflictsWriter.append(f1.getAtcOperator());
                conflictsWriter.append(f1.getCfn());
                conflictsWriter.append(',');
                conflictsWriter.append(f1.getDepartureAirport());
                conflictsWriter.append(',');
                conflictsWriter.append(f1.getDestinationAirport());                        
                conflictsWriter.append(',');
                conflictsWriter.append(OUT_DATE_FORMAT.format(si.getFirstDayOfOperation()));
                conflictsWriter.append(',');
                conflictsWriter.append(OUT_DATE_FORMAT.format(si.getLastDayOfOperation()));
                conflictsWriter.append(',');
                for (DayOfWeek dow : DayOfWeek.values()) {
                    if (si.getDaysOfOperation().contains(dow)) {
                        conflictsWriter.append(Integer.toString(dow.ordinal()+1));
                    } else{
                        conflictsWriter.append('0');
                    }
                }
                conflictsWriter.append(',');                        
                conflictsWriter.append(DIGIT2_FORMAT.format(f1.getEobt() / 60));
                conflictsWriter.append(DIGIT2_FORMAT.format(f1.getEobt() % 60));
                conflictsWriter.append(',');
                int eta = f1.getEta();
                eta %= 24*60;
                conflictsWriter.append(DIGIT2_FORMAT.format(eta / 60));
                conflictsWriter.append(DIGIT2_FORMAT.format(eta % 60));
            }
            
        });
        
        System.out.println("conflicting flight schedules: " + conflictingSchedule.intValue());
        if (conflictsWriter != null) conflictsWriter.close();
        
        phase.stop();
        
         // System.out.println(conflicts.size());
        

        global.stop();
    }

    private static void fillConflict(BufferedReader reader, Map<String, String> adaConflicts) throws IOException {
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] fields = line.split(",");
            String[] left = ArrayUtils.subarray(fields, 0, 8);
            String[] right = ArrayUtils.subarray(fields, 8, 16);
            String leftk = StringUtils.join(left, ',');
            String rightk = StringUtils.join(right, ',');
            
            if (leftk.compareTo(rightk) < 0) {
                adaConflicts.put(leftk + rightk, line);
            } else {
                adaConflicts.put(rightk + leftk, line);                
            }
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(80);
        formatter.printHelp("java -cp PATH/prototype-0.0.1-SNAPSHOT-jar-with-dependencies.jar " + Conflict.class.getCanonicalName() + " [options] CSV_INPUT_FILE CSV_PROFILE_CATALOG", options);
    }

}
