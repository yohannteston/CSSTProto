package com.bull.eurocontrol.csst.poc.utils;

import java.io.Closeable;
import java.io.IOException;

public class Closeables {
    public static void closeSilently(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
