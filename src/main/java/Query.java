
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jsr166y.ForkJoinPool;

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

import com.bull.aurocontrol.csst.poc.ConflictQuery;
import com.bull.aurocontrol.csst.poc.DayOfWeek;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightPairData;
import com.bull.aurocontrol.csst.poc.FlightSchedule;
import com.bull.aurocontrol.csst.poc.FlightSeason;
import com.bull.aurocontrol.csst.poc.index.LuceneIndexFlightSeason;
import com.bull.aurocontrol.csst.poc.index.OverlapProcessParameters;
import com.bull.aurocontrol.csst.poc.index.LuceneIndexSeasonFactory;
import com.bull.aurocontrol.csst.poc.index.rules.AnagramsRule;
import com.bull.aurocontrol.csst.poc.index.rules.IdenticalFinalDigitsRule;
import com.bull.aurocontrol.csst.poc.index.rules.ParallelCharactersRule;
import com.bull.eurocontrol.csst.poc.source.CSVFlightSourceFactory;
import com.bull.eurocontrol.csst.poc.utils.IJClosure;
import com.bull.eurocontrol.csst.poc.utils.IJTransformer;
import com.bull.eurocontrol.csst.poc.utils.JamonUtils;
import com.bull.eurocontrol.csst.poc.utils.MemoryWatch;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class Query {

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
        options.addOption("qt", "query-thread", true, "number of search client thread (default: 1)");
        // biz option
        options.addOption("b", "buffer", true, "buffer time for overlap calcs (default: 0)");
        options.addOption("o", "query_output", true, "output results of the queries");
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
            final File queries = new File(cmd.getArgs()[2]);
            
            final int repeat = NumberUtils.toInt(cmd.getOptionValue("r", "1"));
            int threads = NumberUtils.toInt(cmd.getOptionValue("t", "1"));
            int queryThreads = NumberUtils.toInt(cmd.getOptionValue("qt", "1"));
            final int buffer = NumberUtils.toInt(cmd.getOptionValue("b", "0")) / 2;
            
            final File result = (cmd.hasOption("o")) ? new File(cmd.getOptionValue("o")) : null;
            File jamon = (cmd.hasOption("j")) ? new File(cmd.getOptionValue("j")) : null;
            
            ForkJoinPool pool = new ForkJoinPool(threads);
            System.out.println("Warming up: we build the index. In normal operation it is yet in memory.");
            MemoryWatch memoryWatch = new MemoryWatch();
            memoryWatch.start();
            
            CSVFlightSourceFactory sourceFactory = new CSVFlightSourceFactory(input, catalog);

            LuceneIndexSeasonFactory factory = new LuceneIndexSeasonFactory(1, true);

            Iterator<Flight> source = sourceFactory.iterate();

            final LuceneIndexFlightSeason season =  (LuceneIndexFlightSeason) factory.buildFlightSeason(source, buffer, pool);
            
            pool.shutdown();
            memoryWatch.stop();
            
            System.out.println("Indexes are in memory. Current jvm heap size is :" + memoryWatch.toString());
            
            MonitorFactory.reset();
            System.out.printf("Now reperating the query list %s times and take the performance measures\n", repeat);
           
            ExecutorService executorService = Executors.newFixedThreadPool(queryThreads);
            for (int i = 0; i < queryThreads; i++) {
                final int id = i;
                executorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            doTest(queries, repeat, result, season, id);
                        } catch (FileNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (java.text.ParseException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        
                    }
                    
                });
            }
            
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.DAYS);

            JamonUtils.outputJamonReport(jamon, "ms.", false);
            JamonUtils.outputJamonReport(jamon, "count", true);
            
            

        } catch (Exception e) {
            printHelp(options);
            throw e;
        }
    }



    private static void doTest(File queries, int repeat, File result, final LuceneIndexFlightSeason season, int id) throws IOException, FileNotFoundException,
            java.text.ParseException {
        BufferedWriter resultOutput = null;
        
        if (result != null) {
            result = new File(result.getParent(), "thread-"+id+"-" + result.getName());
            resultOutput = new BufferedWriter(new FileWriter(result));
        }
        
        for (int i = 0; i < repeat; i++) {
            BufferedReader queriesReader = new BufferedReader(new FileReader(queries));
            for (String line = queriesReader.readLine(); line != null && line.contains(","); line = queriesReader.readLine()) {
                String[] fields = StringUtils.split(line, ',');
                String adep = (fields[0].equals("*")) ? null : fields[0];
                String ades = (fields[1].equals("*")) ? null : fields[1];
                Date from = (fields[2].equals("*")) ? null : new SimpleDateFormat("yyyyMMdd").parse(fields[2]);
                Date to = (fields[3].equals("*")) ? null : new SimpleDateFormat("yyyyMMdd").parse(fields[3]);
                int minimumConflicts = (fields[4].equals("*")) ? 0 : Integer.parseInt(fields[4]);

                ConflictQuery q = new ConflictQuery(adep, ades, from, to, minimumConflicts);
                
                String detailKey = "query";
                detailKey += "/" + ((adep != null) ? "ADEP" : "*");
                detailKey += "/" + ((ades != null) ? "ADES" : "*");
                detailKey += "/" + ((from != null) ? (to.getTime() - from.getTime()) / (24*60*60*1000) : "*");
                detailKey += "/" + minimumConflicts;
                Monitor query = MonitorFactory.startPrimary("query");
                
                
                Monitor detail = MonitorFactory.start(detailKey);
                SMatrix<FlightPairData> conflicts = season.queryConflicts(q);
                detail.stop();
                query.stop();
                int size = conflicts.size();

                MonitorFactory.add("query", "count", (double) size);
                MonitorFactory.add(detailKey, "count", (double) size);
                
                System.out.println("QUERY:" + q + " results:" + size);
                
                if (result != null) {
                    resultOutput.append("QUERY:" + q + "\n\n");
                    resultOutput.append(conflicts.toString());
                    resultOutput.append("\n\n" + StringUtils.repeat('*', 80) + "\n");                        
                }
            }
            queriesReader.close();
            
            
        }
        resultOutput.close();
    }

 

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(80);
        formatter.printHelp("java -cp PATH/prototype-0.0.1-SNAPSHOT-jar-with-dependencies.jar " + Query.class.getCanonicalName() + " [options] CSV_INPUT_FILE CSV_PROFILE_CATALOG", options);
    }

}
