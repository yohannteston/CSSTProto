package com.bull.eurocontrol.csst.poc.utils;

public class MemoryWatch {

    private long startMemory;
    private long stopMemory;


    public void start() {
        startMemory = getUsedMemory();
    }

    public void stop() {
        stopMemory = getUsedMemory();
    }
    
    
    public String toString() {
        long totalUsed = stopMemory - startMemory;
        if (totalUsed < 1024 * 1024) {
            return (totalUsed) + "KB";
        } else {
            return (totalUsed / 1024) + "KB";
        }
    }

    private static long getUsedMemory() {
        gc();
        long totalMemory = Runtime.getRuntime().totalMemory();
        gc();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long usedMemory = totalMemory - freeMemory;
        return usedMemory;
    }

    public static void gc() {
        try {
            System.gc();
            Thread.currentThread().sleep(100);
            System.runFinalization();
            Thread.currentThread().sleep(100);
            System.gc();
            Thread.currentThread().sleep(100);
            System.runFinalization();
            Thread.currentThread().sleep(100);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
