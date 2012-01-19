import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;


public class GenerateQueries {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        BufferedWriter writer =new BufferedWriter(new FileWriter("Results/queries.csv", false));
        
        String[] adeps = new String[] { "*", "EHAM", "LEZL", "LEMD", "LEGR", "LFMN"};
        String[] adess = new String[] { "*" };
        int[] mins = new int[] { 0, 5, 10, 50 };
        
        Date from = new Date(1301184000000L);
        Date to = new Date(1306627200000L);
        
        Random random = new Random(1L);
        FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd");
        DecimalFormat num = new DecimalFormat("00");
        
        for (String adep : adeps) {
            
            for (String ades : adess) {

                for (int min : mins) {
                    
                    add(writer, adep, ades, "*", "*", num.format(min));
                    
                    for (Date dayCursor = from; dayCursor.before(to); dayCursor = DateUtils.addDays(dayCursor, random.nextInt(7))) {
                        add(writer, adep, ades, format.format(dayCursor), format.format(DateUtils.addDays(dayCursor, 1)), num.format(min));                        
                    }
                    for (Date dayCursor = from; dayCursor.before(to); dayCursor = DateUtils.addDays(dayCursor, 7 + random.nextInt(7))) {
                        add(writer, adep, ades, format.format(dayCursor), format.format(DateUtils.addDays(dayCursor, 7)), num.format(min));                        
                    }
                    
                    
                }
                
            }
        }
        
        writer.close();

    }

    private static void add(BufferedWriter writer, String... fields) throws IOException {
        writer.append(StringUtils.join(fields,','));
        writer.newLine();
    }

}
