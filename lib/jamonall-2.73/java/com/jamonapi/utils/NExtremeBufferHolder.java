package com.jamonapi.utils;


import java.util.*;

/** Buffer used to keep the last N recent values based on the comparator.  Note the Comparator must
 * be thread safe.
 * @author steve souza
 *
 */
public class NExtremeBufferHolder implements BufferHolder, Comparator {

	private Object nextToRemove;
	private List list=new ArrayList();
	
	private static final int EQUALTO=0;
	private static final int GREATERTHAN=1;
	private Comparator comparator;


	public  NExtremeBufferHolder(Comparator comparator) {
		this.comparator=comparator;
		
	}

	public NExtremeBufferHolder(boolean naturalOrder) {
		comparator=new JAMonComparator(naturalOrder);
	}
	
	public List getCollection() {
		return list;
	}
	
	
	public void setComparator(Comparator comparator) {
		this.comparator=comparator;
	}
	
	public Comparator getComparator() {
		return comparator;
	}

	
	private Object getNextToRemove() {
		  return Collections.min(list, comparator);		  
   }
	
	private boolean isTrue(int newVal) {
		return (newVal==GREATERTHAN  || newVal==EQUALTO) ? true : false;
		  
	}


	/**
	 * Method used by the comparator interface. <br><br>
	 *   o1 &lt; o2 - returns a negative integer<br>
	 *   o1==o2 - returns zero<br>
	 *   o1>o2 - returns a postitive integer<br><br>
	 *  Iterate through all columns that should be compared (in the proper order) 
	 *  and call the Comparator for each of the column elements.
	 * Note the column value is always the first argument and the comparison value is always the second
	 * 
	 *
	 */
	
	/*
	 * Returns 
	 * this object is less than - returns a negative integer, 
	 * this object equal to - retunrs zero
     * this object greater than - return positive integer 
	 */
	public int compare(Object newObj, Object existingObj) {
	  	return comparator.compare(newObj, existingObj);
    }

	public void add(Object replaceWithObj) {
		list.add(replaceWithObj);
    }

	public void remove(Object replaceWithObj) {
		  list.remove(nextToRemove);
		  nextToRemove=getNextToRemove();
    }



	public boolean shouldReplaceWith(Object replaceWithObj) {
		if (nextToRemove==null)
			nextToRemove=getNextToRemove();
		
		return isTrue(compare(replaceWithObj, nextToRemove));
    }

	public List getOrderedCollection() {
		Collections.sort(list, comparator);
	    return list;
    }

	public void setCollection(List list) {
		this.list=list;
	    
    }

	public BufferHolder copy() {
	    return new NExtremeBufferHolder(comparator);
    }

}
