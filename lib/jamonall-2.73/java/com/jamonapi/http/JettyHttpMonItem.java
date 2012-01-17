package com.jamonapi.http;

/** 
 *  used to monitor jetty requests via the JAMonJettyHandler.
 */
import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import org.mortbay.jetty.Request;


class JettyHttpMonItem extends HttpMonItem {

    JettyHttpMonItem() {
    }
    
    
    JettyHttpMonItem(String label, HttpMonFactory httpMonFactory) {
        super(label, httpMonFactory);
        
    }

    
    /** Jetty Handlers does not let jamon start/stop time them.  It seems the request is done by the time jamon gets it.  To overcome this use the jetty api 
     * to get the time of a request for a page.  If it isn't a jetty request then call the parent.
     */

    // Only called if this is a time monitor i.e units are 'ms.'
    Monitor startTimeMon(HttpMonRequest httpMonBase) {
        if (httpMonBase.getRequest() instanceof Request)            
          return MonitorFactory.getMonitor(getMonKey(httpMonBase)).start();
        else 
          return super.startTimeMon(httpMonBase);
    }
    
    
    // Only called if this is a time monitor i.e units are 'ms.'
    void stopTimeMon(HttpMonRequest httpMonBase) {
        if (httpMonBase.getRequest() instanceof Request)  {
          Request request=(Request)httpMonBase.getRequest();
          Monitor mon=httpMonBase.getNextTimeMon();
          if (mon!=null) {
             mon.add(System.currentTimeMillis()-request.getTimeStamp()).stop();// figure elapsed time and then decrement active.
          }
        } else
            super.stopTimeMon(httpMonBase);
     }
    
 }
