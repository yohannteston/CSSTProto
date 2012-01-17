package com.jamonapi;

/** key that allows for a monitor to be passed any number of keys used in the equivalent
 * of a group by clause.  Put in hashmap to identify a Monitor.  Implementations  will need 
 * to implement equals, and hashcode.   MonKeys are the way Monitors are identified in the 
 * storing Map
 */



public interface MonKey extends RowData, MonKeyItem {
    final String LABEL_HEADER="Label";
    final String UNITS_HEADER="Units";
    
    /** return any value associated with the key.  
     * new MonKey(label, units).  would return the value associated with label
     * or units if:  getValue("label"), or getValue("units");
     */
    public Object getValue(String primaryKey);
    /** Uses this value to look up an associated Range */
    public String getRangeKey();
    public String getLabel();
    
}
