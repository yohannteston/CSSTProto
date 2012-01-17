package com.jamonapi.http;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;


/** Note this servlet filter is more functional than com.jamonapi.JAMonFilter, but either can be used.   
 * This is a wrapper class for the true monitoring class of HttpMonFactory.
 * 
 * <web-app>
   <display-name>jamon</display-name>
   <filter>
    <filter-name>JAMonServletFilter</filter-name>
    <filter-class>com.jamonapi.http.JAMonServletFilter</filter-class>
   </filter>
    
   <filter-mapping>
     <filter-name>JAMonServletFilter</filter-name>
     <url-pattern>*</url-pattern>
   </filter-mapping>
   */
public class JAMonServletFilter extends HttpServlet implements HttpMonManage, Filter {

    private static final long serialVersionUID = 27L;

    private static final String PREFIX="com.jamonapi.http.JAMonServletFilter";
    private HttpMonFactory httpMonFactory=new HttpMonFactory(PREFIX);
      
    private final String jamonSummaryLabels="request.getRequestURI().ms as allPages, request.getRequestURI().value.ms as page, request.contextpath.ms";
    
    public JAMonServletFilter() {
        setSummaryLabels(jamonSummaryLabels);
    }
    
    /** Servlet filter method that does the monitoring */

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        HttpMon httpMon=null;
        try {
           httpMon=httpMonFactory.start(request, response);
           filterChain.doFilter(request, response);
        } catch (Throwable e) {
           httpMon.throwException(e);
        } finally {
           httpMon.stop();
        }
    }
    
    public void setSummaryLabels(String jamonSummaryLabels) {
        httpMonFactory.setSummaryLabels(jamonSummaryLabels);
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

    public void init(FilterConfig arg0) throws ServletException {
     
    }


}
