package com.bull.eurocontrol.csst.poc.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.lang3.StringUtils;

import com.jamonapi.MonitorFactory;

public class JamonUtils {
    public static void outputJamonReport(File jamon) throws IOException {
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
        if (jamon != null) jamonWriter.close();
        
    }

}
