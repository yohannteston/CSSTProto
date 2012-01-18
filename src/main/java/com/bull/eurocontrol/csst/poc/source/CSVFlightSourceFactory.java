package com.bull.eurocontrol.csst.poc.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.TransformerUtils;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.collections.map.TransformedMap;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.FastDateFormat;


import com.bull.aurocontrol.csst.poc.AirspaceProfile;
import com.bull.aurocontrol.csst.poc.DayOfWeek;
import com.bull.aurocontrol.csst.poc.Flight;
import com.bull.aurocontrol.csst.poc.FlightProfile;
import com.bull.aurocontrol.csst.poc.FlightSchedule;
import com.bull.aurocontrol.csst.poc.FlightSourceFactory;

public class CSVFlightSourceFactory implements FlightSourceFactory {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }
   

    private File file;

    private File profilesFile;


    


    public CSVFlightSourceFactory(File file, File profiles) {
        super();
        this.file = file;
        this.profilesFile = profiles;
        validateFile(file);
        validateFile(profiles);
    }




    private void validateFile(File file) {
        if (!file.exists()) throw new IllegalArgumentException("file doesn't exist:" + file);
        if (file.isDirectory()) throw new IllegalArgumentException("file is a directory:" + file);
        if (!file.canRead()) throw new IllegalArgumentException("no permission to read file:" + file);
    }


    private final class CSVFlightIterator implements Iterator<Flight> {
        private final Map<String, FlightProfile> profiles;
        private final CSVCursor cursor;
        int id = 0;
        Flight f;
        boolean done = false;

        private CSVFlightIterator(Map<String, FlightProfile> profiles, CSVCursor cursor) throws IOException, ParseException {
            this.profiles = profiles;
            this.cursor = cursor;
            f = load();
        }

        @Override
        public boolean hasNext() {
           return f != null;
        }

        private Flight load() throws IOException, ParseException {
            if (done) return null;
            Flight f = new Flight();
            f.setId(id++);
            f.setCfn(cursor.cfn);
            f.setAtcOperator(cursor.atcOperator);
            f.setAtcFlightId(cursor.atcFlightId);                    
            f.setDepartureAirport(cursor.departureAirport);
            f.setDestinationAirport(cursor.destinationAirport);
            f.setEobt(cursor.eobt);
            f.setEta(cursor.eta);
            
            
            f.setProfile(profiles.get(cursor.departureAirport + '-' + cursor.destinationAirport));
  
            TreeSet<FlightSchedule> schedules = new TreeSet<FlightSchedule>();
          
            do {
                schedules.add(buildSchedule(cursor));

                if (cursor.next()) {
                    if (!f.getCfn().equals(cursor.cfn) 
                            || !f.getAtcOperator().equals(cursor.atcOperator)
                            || !f.getDepartureAirport().equals(cursor.departureAirport)
                            || !f.getDestinationAirport().equals(cursor.destinationAirport)
                            || f.getEobt() != cursor.eobt
                            || f.getEta() != cursor.eta                                    
                            ) {
                        break;
                    }
                } else {
                    done = true;
                    break;
                }
                
            } while (true);
            
            f.setSchedules(schedules.toArray(schedules.toArray(new FlightSchedule[schedules.size()])));
            return f;
        }

        private FlightSchedule buildSchedule(final CSVCursor cursor) {
            FlightSchedule s = new FlightSchedule();
            s.setDaysOfOperation(cursor.daysOfOperation);
            s.setFirstDayOfOperation(cursor.firstDayOfOperation);
            s.setLastDayOfOperation(cursor.lastDayOfOperation);
            return s;
        }

        @Override
        public Flight next() {
            Flight result = f;
            try {
                f = load();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();

        }
    }




    private static class CSVCursor {
        private final BufferedReader csv;
        
        private String cfn;
        private String atcOperator;
        private String atcFlightId;
        private String departureAirport;
        private String destinationAirport;
        private long firstDayOfOperation;
        private long lastDayOfOperation;
        private EnumSet<DayOfWeek> daysOfOperation;
        private int eobt;
        private int eta;

        
        
        public CSVCursor(File file) throws IOException, ParseException {
            csv = new BufferedReader(new FileReader(file));
            csv.readLine();
            next();
        }
        
        public boolean next() throws IOException, ParseException {
            String line = csv.readLine();
            if (line == null) return false;
            String[] row = line.split(",");
            
            String tmp = row[1] + row[2];
            if (!tmp.equals(cfn)) cfn = tmp.intern();

            tmp = row[3];
            if (!tmp.equals(atcOperator)) atcOperator = tmp.intern();
            
            tmp = row[4];
            if (!tmp.equals(atcFlightId)) atcFlightId = tmp.intern();
            
            tmp = row[8];
            if (!tmp.equals(departureAirport)) departureAirport = tmp.intern();
            
            tmp = row[10];
            if (!tmp.equals(destinationAirport)) destinationAirport = tmp.intern();

            firstDayOfOperation = parse(row[5]);
            lastDayOfOperation = parse(row[6]);
            
            daysOfOperation = parseDOO(row[7]);
            
            int eobt = Integer.parseInt(row[9]);
            eobt = (eobt / 100 * 60) + eobt % 100;
            this.eobt = eobt;
            
            int eta = Integer.parseInt(row[11]);
            eta = (eta / 100 * 60) + eta % 100;
            if (eta < eobt) eta = 60*24 + eta;
            this.eta = eta;
            
            return true;
        }
        
        
        private EnumSet<DayOfWeek> parseDOO(String value) {
            char[] dayOp = value.toCharArray();
            EnumSet<DayOfWeek> dow = EnumSet.noneOf(DayOfWeek.class);
            for (char c : dayOp) {
                if (Character.isDigit(c)) {
                    dow.add(DayOfWeek.values()[Character.digit(c, 10) - 1]);
                }
            }
                
            return dow;
        }

        private SimpleDateFormat dateParser = new SimpleDateFormat("ddMMMyy", Locale.ENGLISH);
        
        private long parse(String value) throws ParseException {
            Date date = dateParser.parse(value);
            
            return date.getTime();
        }        
        
    }
    


    @Override
    public Iterator<Flight> iterate() {
        
        try {
            final Map<String, FlightProfile> profiles = loadProfiles();
        
            final CSVCursor cursor = new CSVCursor(file);
            
      
            final LinkedHashMap<MultiKey, Flight> grouping = new LinkedHashMap<MultiKey, Flight>();
            
            for (Iterator<Flight> iterator = new CSVFlightIterator(profiles, cursor); iterator.hasNext();) {
                Flight f = iterator.next();
                MultiKey key = new MultiKey(f.getCfn(), f.getAtcOperator(),f.getDepartureAirport(),f.getDestinationAirport(),f.getEobt());
                
                Flight pf = grouping.get(key);
                if (pf != null) {
                    TreeSet<FlightSchedule> schedules = new TreeSet<FlightSchedule>();
                    CollectionUtils.addAll(schedules, pf.getSchedules());
                    CollectionUtils.addAll(schedules, f.getSchedules());
                    pf.setSchedules(schedules.toArray(schedules.toArray(new FlightSchedule[schedules.size()])));
                } else {
                    grouping.put(key, f);
                }
                
            }
            
            
            return new Iterator<Flight>() {
                Iterator<Flight> flights = grouping.values().iterator();
                
                @Override
                public boolean hasNext() {                   
                    return flights.hasNext();
                }

                @Override
                public Flight next() {
                    Flight r = flights.next();
                    flights.remove(); // allow clean up
                    return r;
                }

                @Override
                public void remove() {
                }
            };
                   
//            return new CSVFlightIterator(profiles, cursor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }




    private Map<String, FlightProfile> loadProfiles() throws FileNotFoundException, IOException {
        final Map<String, FlightProfile> profiles = new HashMap<String, FlightProfile>();
        final BufferedReader pcsv = new BufferedReader(new FileReader(profilesFile));
        pcsv.readLine(); // skip header
        
        for (String line = pcsv.readLine(); line != null; line = pcsv.readLine()) {
            final String[] row = line.split(",");
            if ("J".equals(row[2])) {
                final String key = row[0] + '-' + row[1];
                final int taxitime = Integer.parseInt(row[3]);
                final int duration = Integer.parseInt(row[4]);
                final int airspaceCount = Integer.parseInt(row[7].trim());
                
                final AirspaceProfile[] airspaces = new AirspaceProfile[airspaceCount];
                for (int i = 0; i < airspaceCount; i++) {
                    final String name = row[8+i*3].intern();
                    final int start = Integer.parseInt(row[9+i*3]);
                    final int asDuration = Integer.parseInt(row[10+i*3]);
                    
                    airspaces[i] = new AirspaceProfile(name, start, asDuration);
                    
                }
                
                assert (!profiles.containsKey(key));
                
                 Arrays.sort(airspaces, new Comparator<AirspaceProfile>() {

                    @Override
                    public int compare(AirspaceProfile o1, AirspaceProfile o2) {
                        return o1.getStart() - o2.getStart();
                    }
                     
                 });
                profiles.put(key, new FlightProfile(duration, taxitime, airspaces));
            }
        }
        return profiles;
    }
    

}
