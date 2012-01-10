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
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;


import com.bull.aurocontrol.csst.poc1.AirspaceProfile;
import com.bull.aurocontrol.csst.poc1.DayOfWeek;
import com.bull.aurocontrol.csst.poc1.Flight;
import com.bull.aurocontrol.csst.poc1.FlightProfile;
import com.bull.aurocontrol.csst.poc1.FlightSourceFactory;

public class CSVFlightSourceFactory implements FlightSourceFactory {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }
    private static final ThreadLocal<SimpleDateFormat> dateParser = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("ddMMMyy", Locale.ENGLISH);
        }
    };

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




    @Override
    public Iterator<Flight> iterate() {
        
        try {
            final Map<String, FlightProfile> profiles = new HashMap<String, FlightProfile>();
            final BufferedReader pcsv = new BufferedReader(new FileReader(profilesFile));
            pcsv.readLine(); // skip header
            
            for (String line = pcsv.readLine(); line != null; line = pcsv.readLine()) {
                final String[] row = line.split(",");
                if ("J".equals(row[2])) {
                    final String key = row[0] + '-' + row[1];
                    final int duration = Integer.parseInt(row[4]);
                    final int airspaceCount = Integer.parseInt(row[7]);
                    
                    final AirspaceProfile[] airspaces = new AirspaceProfile[airspaceCount];
                    for (int i = 0; i < airspaceCount; i++) {
                        final String name = row[8+i*3].intern();
                        final int start = Integer.parseInt(row[9+i*3]);
                        final int asDuration = Integer.parseInt(row[10+i*3]);
                        
                        airspaces[i] = new AirspaceProfile(name, start, asDuration);
                        
                    }
                    
                    assert (!profiles.containsKey(key));
                    profiles.put(key, new FlightProfile(duration, airspaces));
                }
            }
        
        
            final BufferedReader csv = new BufferedReader(new FileReader(file));
            csv.readLine(); // skip header
            
            return new Iterator<Flight>() {
                int id = 0;

                @Override
                public boolean hasNext() {
                   
                    try {
                        return csv.ready();
                    } catch (IOException e) {
                        e.printStackTrace();
                        return false;
                    }
                }

                @Override
                public Flight next() {
                    
                    try {
                        String[] row = csv.readLine().split(",");
                        Flight flight = new Flight();
                        flight.setId(id++);
                        flight.setAtcOperator(row[3].intern());
                        flight.setAtcFlightId(row[4].intern());                    
                        flight.setFirstDayOfOperation(parse(row[5]));
                        flight.setLastDayOfOperation(parse(row[6]));
                        
                        
                        char[] dayOp = row[7].toCharArray();
                        EnumSet<DayOfWeek> dow = EnumSet.noneOf(DayOfWeek.class);
                        for (char c : dayOp) {
                            int d = c - 49;
                            dow.add(DayOfWeek.values()[d]);
                        }
                        flight.setDaysOfOperation(dow);
                        String departureAirport = row[8].intern();
                        flight.setDepartureAirport(departureAirport);
                        String destinationAirport = row[10].intern();
                        flight.setDestinationAirport(destinationAirport);

                        int eobt = Integer.parseInt(row[9]);
                        eobt = (eobt / 100 * 60) + eobt % 100;
                        int eta = Integer.parseInt(row[11]);
                        eta = (eta / 100 * 60) + eta % 100;
                        if (eta < eobt) eta = eobt + eta;

                        flight.setEobt(eobt);
                        flight.setEta(eta);
                        FlightProfile profile = profiles.get(departureAirport + '-' + destinationAirport);
                       
                                               
                        //assert (profile != null) : "no profile for " + flight;
                        
                        flight.setProfile(profile);
                        
                        
                        return flight;
                    } catch (NumberFormatException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();

                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static Date parse(String value) throws ParseException {
        Date date = dateParser.get().parse(value);
        
        return date;
    }

}
