package com.jamonapi.proxy;

import java.lang.reflect.*;
import java.sql.SQLException;
import java.util.*;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import com.jamonapi.MonKeyImp;
import com.jamonapi.utils.*;

/** 
 * By using this proxy class ANY java interface can be monitored for performance and exceptions via JAMon.
 * In particular the 'monitor' method is great for wrapping (and so monitoring) any of the JDBC classes which
 * all implement interfaces (i.e. Connection, ResultSet, Statement, PreparedStatement,...).  It tracks performance
 * stats (hits/avg/min/max/...) for method calls and Exceptions.
 * 
 * Sample code:
 *   ResultSet rs= MonProxyFactory.monitor(resultSet);
 *   Connection conn=MonProxyFactory.monitor(connection);
 *   MyInterface my=(MyInterface) MonProxyFactory.monitor(myObject);//myObject implements MyInterface
 *   YourInterface your=(YourInterface) MonProxyFactory.monitor(yourObject);//myObject implements MyInterface
 *
 * Subsequent public method calls on this interface will be monitored.  Quite cool.   If the proxy is disabled
 * the Object will be returned unchanged.
 * 
 * Note the object passed MUST implement an interface or a runtime exception will occur i..e it can't be a 
 * plain java object.
 * 
 * Also, to change from the default labeler interface you must do the following on your monitored object.
 *       Map map=(Map) MonProxyFactory.monitor(new HashMap());
 *       MonProxy monProxy=MonProxyFactory.getMonProxy((Proxy)map);
 *       monProxy.setLabeler(new MyWonderfulLabeler());

 *
 */
public class MonProxy implements InvocationHandler {

	private Object monitoredObj;// underlying object
	Params params;// parameters associated with monitoring
	private MonProxyLabelerInt labelerInt;

	private static Method EQUALS_METHOD;
	static {

		try {
			EQUALS_METHOD = Object.class.getMethod("equals",
					new Class[] { Object.class });
		} catch (Exception e) {
			Logger.log("Error trying to create reflective equals method.  This error should never happen: "+e);
		}

	}

	
    MonProxy(Object monitoredObj, Params params, MonProxyLabelerInt labelerInt) {
        this.monitoredObj = monitoredObj;
        this.params = params;
        this.labelerInt=labelerInt;
        this.labelerInt.init(this);
    }
	

	/** Return the underlying object being Monitored.  If the monitored object is wrapped with another Monitored object
	 * it will call until MonProxy is not returned (i.e. find the underlying monitored object */
	public Object getMonitoredObject() {
		  return getMonitoredObject(monitoredObj);
	}

	/** Static method that returns the underlying/wrapped/monitored object if the passed object is an interfaced monitored by JAMon else
	 *  if not simply return the object unchanged.  It will follow the chain of MonProxy objects until
	 *  the first non-MonProxy object is returned.  This could return another type of Proxy object too.
	 *   */
	public static Object getMonitoredObject(Object obj) {

		if (obj == null)
			return null;
		
		// loop until the monitored object is not a MonProxy object.
		// note this is a recursive call.
		while (Proxy.isProxyClass(obj.getClass())
				&& (Proxy.getInvocationHandler(obj) instanceof MonProxy)) {
					MonProxy monProxy = (MonProxy) Proxy.getInvocationHandler(obj);
					obj=monProxy.getMonitoredObject();
		}
		
		return obj;

	}
	
	

	/** Set labeler to be called when summary stats are recorded for a method call or when the interface throws an exception */
	
    public void setLabeler(MonProxyLabelerInt labelerInt) {
          this.labelerInt=labelerInt;
    }
 
    /** Get current labeler */
    public MonProxyLabelerInt getLabeler() {
        return labelerInt;
    }

	/** Method that monitors method invocations of the proxied interface.  This method is not explicitly called.  
	 *  The MonProxy class automatically calls it. 
	 *  
	 *  
	 */
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {

		Monitor mon = null;
		boolean isExceptionSummaryEnabled = (params.isExceptionSummaryEnabled && params.isEnabled);// track jamon stats for Exceptions?
		boolean isExceptionDetailEnabled = (params.isExceptionDetailEnabled && params.isEnabled);// save detailed stack trace in the exception buffer?
		// Because this monitor string is created every time I use a StringBuffer as it may be more effecient.
		// I didn't do this in the exception part of the code as they shouldn't be called as often.
		if (params.isInterfaceEnabled && params.isEnabled) {
	          mon = MonitorFactory.start(labelerInt.getSummaryLabel(method));
		}

		try {
			// Special logic must be performed for 'equals'.  If not nonproxy.equals(proxy)
			// will not return true
			if (method.equals(EQUALS_METHOD))
			   return Boolean.valueOf(equals(args[0]));
			else
			   return method.invoke(monitoredObj, args);// executes underlying interfaces method;
		} catch (InvocationTargetException e) {

			if (isExceptionSummaryEnabled || isExceptionDetailEnabled) {
				String sqlMessage = "";
				String detailStackTrace=null;
				Throwable rootCause = e.getCause();

				// Add special info if it is a SQLException
				if (rootCause instanceof SQLException
						&& isExceptionSummaryEnabled) {
					SQLException sqlException = (SQLException) rootCause;
					sqlMessage = ",ErrorCode=" + sqlException.getErrorCode()
							+ ",SQLState=" + sqlException.getSQLState();
				}

				if (isExceptionSummaryEnabled) {
					// Add jamon entries for Exceptions
					detailStackTrace=Misc.getExceptionTrace(rootCause);
                    
                    MonitorFactory.add(new MonKeyImp(
                            MonitorFactory.EXCEPTIONS_LABEL, 
                            detailStackTrace, 
                            "Exception"), 1); // counts total exceptions from jamon  
					MonitorFactory.add(new MonKeyImp(
							"MonProxy-Exception: InvocationTargetException",
							detailStackTrace,
							"Exception"), 1); //counts total exceptions for MonProxy
					MonitorFactory.add(new MonKeyImp(
							"MonProxy-Exception: Root cause exception="
									+ rootCause.getClass().getName()
									+ sqlMessage, 
									detailStackTrace,									
									"Exception"), 1); // Message for the exception
					MonitorFactory.add(new MonKeyImp(
							labelerInt.getExceptionLabel(method), 
							detailStackTrace,
							"Exception"),
							1); // Exception and method that threw it.


				}

				// Add stack trace to buffer if it is enabled.
				if (isExceptionDetailEnabled) {
				    if (detailStackTrace==null)
				      detailStackTrace=Misc.getExceptionTrace(rootCause);
				    
					params.exceptionBuffer.addRow(new Object[] {
							new Long(++params.exceptionID), new Date(),
							detailStackTrace, method.toString(), });
				}
			} // end if (enabled)           

			throw e.getCause();

		} finally {
			if (mon != null)
				mon.stop();
		}
	}

	
	/** When this is called on the proxy object it is the same as calling
	 * proxyObject1.equals(proxyObject2) is the same as calling originalObject1.equals(originalObject2)
	 * 
	 */
	public boolean equals(Object obj) {
		return getMonitoredObject().equals(getMonitoredObject(obj));
	}
	
	


}
