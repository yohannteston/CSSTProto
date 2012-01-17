package com.jamontomcat;


 

/** Note this is simply a copy of com.jamonapi.http.JAMonTomcatValve. Tomcat 5.5 would not work with the valve in the same jar as the jamon classes
 * and also display stats in jamon.war.  Simply compile this class sepeartely and put it in tomcats /server/classes/com/jamontomcatvalve/http and put jamon-2.7.jar or higher in tomcats
 * common/lib directory.   Note this class should also work in tomcat 6 although it is easier simply to put jamon-2.7.jar (or higher in the server/lib)
 * for tomcat 6.
 * 
 *  <Valve className="com.jamonapi.http.JAMonTomcatValve"/>
*   same as default above
*    <Valve className="com.jamonapi.http.JAMonTomcatValve" summaryLabels="request.getRequestURI().ms, response.getContentCount().bytes, response.getStatus().value.httpStatus">
 *  <Valve className="com.jamonapi.http.JAMonTomcatValve  summaryLabels="request.getRequestURI().ms, request.getRequestURI().value.ms, response.getContentCount().pageBytes,response.getStatus().httpStatusCode, response.getStatus().value.httpStatusCode,     response.getContentType().value.type"/>  
  * @author steve souza
 *
 */


//START tomcat 4/5
/*
  
Note I have not tested or run TomcatValves in tomcat 4 or 5, however they will work if coded
in a manner similar to the following.
// tomcat 4/5
import org.apache.catalina.*;
import org.apache.catalina.valves.*;

// use catalina.jar from tomcat4

public class JAMonTomcat55Valve extends org.apache.catalina.valves.ValveBase {

      // tomcat 4/5
    public void invoke(Request request, Response response,  ValveContext valveContext) throws IOException, ServletException {
         start monitoring      

          // tomcat 4/5
          if (valveContext!=null)
            valveContext.invokeNext(request, response);

         stop monitoring
    }

*/

import javax.servlet.*;

import org.apache.catalina.Valve;
import java.io.IOException;

//START tomcat 5.5/6
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import com.jamonapi.http.*;



public class JAMonTomcat55Valve extends org.apache.catalina.valves.ValveBase {
    private static final String PREFIX="com.jamontomcat.http.JAMonTomcat55Valve";
    private static final String DEFAULT_SUMMARY="default, response.getContentCount().bytes, response.getStatus().value.httpStatus, request.contextpath.ms";
    HttpMonFactory httpMonFactory=new HttpMonFactory(PREFIX);

    private final String jamonSummaryLabels="default";
    
    public JAMonTomcat55Valve() {
        setSummaryLabels(jamonSummaryLabels);
    }

    
    /**
     * Extract the desired request property, and pass it (along with the
     * specified request and response objects) to the protected
     * <code>process()</code> method to perform the actual filtering.
     * This method must be implemented by a concrete subclass.
     *
     * @param request The servlet request to be processed
     * @param response The servlet response to be created
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet error occurs
     * http://www.jdocs.com/tomcat/5.5.17/org/apache/catalina/valves/RequestFilterValve.html
     * 
     * log response, request to see what they do.
     * debug mode?
     * test xml - read property
     */

   public void invoke(Request request, Response response) throws IOException, ServletException  {
     HttpMon httpMon=null;
     try {
         httpMon=httpMonFactory.start(request, response);

         Valve nextValve=getNext();
         if (nextValve!=null)
           nextValve.invoke(request, response);
   
     } catch (Throwable e) {
          httpMon.throwException(e);
     } finally {
         httpMon.stop();
     }
                    
   }



    public void setSummaryLabels(String jamonSummaryLabels) {
        httpMonFactory.setSummaryLabels(jamonSummaryLabels, DEFAULT_SUMMARY);
    }

    
    public String getSummaryLabels() {
        return httpMonFactory.getSummaryLabels();
    }
    
    
    public void addSummaryLabel(String jamonSummaryLabel) {
        httpMonFactory.addSummaryLabel(jamonSummaryLabel);
        
    }

    
    public boolean getIgnoreHttpParams() {
        return httpMonFactory.getIgnoreHttpParams();
    }

    public void setIgnoreHttpParams(boolean ignoreHttpParams) {
        httpMonFactory.setIgnoreHttpParams(ignoreHttpParams);
    }

    public void setEnabled(boolean enable) {
        httpMonFactory.setEnabled(enable);
        
    }

    public int getSize() {
        return httpMonFactory.getSize();
    }

    public boolean getEnabled() {
        return httpMonFactory.getEnabled();
    }

    public void setSize(int size) {
        httpMonFactory.setSize(size);
        
    }


    public String getInfo() {
         return PREFIX;
    }

}
