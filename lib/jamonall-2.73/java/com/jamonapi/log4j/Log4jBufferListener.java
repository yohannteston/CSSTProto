package com.jamonapi.log4j;

import org.apache.log4j.spi.LoggingEvent;
import com.jamonapi.JAMonBufferListener;
import com.jamonapi.JAMonListener;
import com.jamonapi.Monitor;
import com.jamonapi.MonKey;
import com.jamonapi.utils.BufferList;
import com.jamonapi.utils.DetailData;
import com.jamonapi.utils.Misc;
import com.jamonapi.utils.DetailDataWrapper;

/**
 * <p>
 * This class can act as a standard JAMonBufferListener/FIFOBuffer or more
 * interestingly if used with log4j it will put in the Buffer data that although
 * designed to work as a Buffer that displays details unique to log4j, if the
 * monitor does not have a Log4jMonKey, it will fallback to Standard
 * JAMonBufferListener behaviour. This makes it so no problems exist if someone
 * inadvertently assigns a Log4jBufferListener to a non log4j entity.
 * </p>
 * 
 * <p>It will create details of the key details (Label (formattedMessage), LoggerName, Level, Threadname, Exception) followed by 
 * lastvalue, active, and the date of the invocation</p>
 * 
 * 
 * @author steve souza
 * 
 */

public class Log4jBufferListener extends JAMonBufferListener {

    private boolean isLog4jMonKey = false;// looks at data to determine if a
                                            // key is log4j. if so

    // header used to display details in the JAMonBufferListener.
//    private static final String[] LOG4J_HEADER = new String[] { "LoggerName",
//        "Level", "ThreadName", "FormattedMessage", "Date", "Exception" };
    private static HeaderInfo log4jHeader=getHeaderInfo(new String[] { "Label", "LoggerName",
            "Level", "ThreadName", "Exception" });


    /**
     * Constructor that creaates this object with its default name (the class
     * name)
     */
    public Log4jBufferListener() {
        super("Log4jBufferListener");
    }

    /** Pass in the jamonListener name */
    public Log4jBufferListener(String name) {
        super(name);
    }

    /** Name the listener and pass in the jamon BufferList to use */
    public Log4jBufferListener(String name, BufferList list) {
        super(name, list);
    }

    private Log4jBufferListener(String name, BufferList list, boolean isLog4jMonKey) {
        this(name, list);
        this.isLog4jMonKey = isLog4jMonKey;
    }

    /**
     * When this event is fired the monitor will be added to the rolling buffer.
     * If it is a log4j monitor the buffer will be specific to log4j fields
     * (i.e.LoggingEvent info such as threadname, formattedmessage, exception
     * stack trace and a few others. If it is not then the super class's
     * processEvent is called.
     * 
     */
    public void processEvent(Monitor mon) {
        MonKey monKey = mon.getMonKey();
        // If the key is a log4j key it has the LoggingEvent in it and we can
        // put that data into the buffer. If the first record is passed and it is a Log4JMonKey then use 
        // more specific log4j detail buffer array.  Note this variable is also used in the header method.
        if (monKey instanceof Log4jMonKey && !isLog4jMonKey)
            isLog4jMonKey = true;

        if (isLog4jMonKey) {
            Log4jMonKey key = (Log4jMonKey) monKey;
            getBufferList().addRow(toArray(key.getLoggingEvent(), mon));
        } else
            super.processEvent(mon);

    }

    /**
     * method that returns an array to use in the Buffer. It can return any
     * sortable objects as long as they match what is returned in the
     * getHeader() method.
     * 
     * @param event
     * @param mon
     * @return Object[]
     */
    protected Object[] toArray(LoggingEvent event, Monitor mon) {
//        return new Object[] {
//                event.getLoggerName(),
//                event.getLevel().toString(),
//                event.getThreadName(),
//                mon.getMonKey().getDetails(),
//                mon.getLastAccess(),
//                (event.getThrowableInformation() == null || event.getThrowableInformation().getThrowable() == null) ? 
//                        "" : Misc.getExceptionTrace(event.getThrowableInformation().getThrowable()) };
    	
    	// populate header with standard monitor data first and after the fact by log4j data
    	Object[] data=log4jHeader.getData(mon);
     	data[0]=mon.getMonKey().getDetails();
    	data[1]=event.getLoggerName();
    	data[2]=event.getLevel().toString();
    	data[3]=event.getThreadName();
    	data[4]=(event.getThrowableInformation() == null || event.getThrowableInformation().getThrowable() == null) ? 
                        "" : Misc.getExceptionTrace(event.getThrowableInformation().getThrowable());
    	
    	return data;

    }
    

 
    /** Makes a usable copy of this BufferListener */
    public JAMonListener copy() {
        return new Log4jBufferListener(getName(), getBufferList().copy(), isLog4jMonKey);
    }

    /** Returns the valid header for display of this buffer */
    public String[] getHeader() {
        if (isLog4jMonKey)
            return log4jHeader.getHeader();
        else
            return getBufferList().getHeader();
    }


	public DetailData getDetailData() {
		DetailData data=super.getDetailData();
		
		if (isLog4jMonKey)
		  return new DetailDataWrapper(log4jHeader.getHeader(), data.getData());  
		else
		  return data;
	}
	

}
