/*
 * Created on Apr 29, 2004 by JFreydank
 * Revisions Apr 29, 2004 by JFreydank
 *   (Describe the changes here)
 */
package com.ikanow.aleph2.util;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.security.service.IkanowV1SecurityServiceTest;


/**
 * This is a helper class for measuring time (and memory) consumption of a program.
 * @author JFreydan
 */
public class ProfilingUtility
{
	private static final Logger logger = LogManager.getLogger(IkanowV1SecurityServiceTest.class);

	private HashMap<String,Long> times = null;	
  private static ProfilingUtility instance;
  private ProfilingUtility(){
  	times = new HashMap<String,Long>();
  }

  public static synchronized ProfilingUtility getInstance(){
  	if(instance == null){
  		instance = new ProfilingUtility();
  	}
  	return instance;
  }
    /** 
     * This method start the time measurement for one key (method Name) , e.g. if you want to track a time, e.g. before a function entry.
     * @param key
     */
	public static synchronized void timeStart(String key){		
		getInstance().times.put(key, new Long(System.currentTimeMillis()));
	}

	
	/** 
	 * This method start the time measurement for one key (method Name) , e.g. if you want to track a time, e.g. before a function entry.
	 * @param key
	 */
	public static void timeStopAndOut(String key){
		long timeStop = System.currentTimeMillis();
		Long timeStart = (Long)getInstance().times.get(key);
		long timeUsed =0;
		if(timeStart!=null){
			timeUsed = timeStop - timeStart.longValue();
		}
		System.out.println("Profiled: "+key+"="+timeUsed);		
	}
	/** 
	 * This method start the time measurement for one key (method Name) , e.g. if you want to track a time, e.g. before a function entry.
	 * @param key
	 */
	public static void timeStopAndLog(String key){
		long timeStop = System.currentTimeMillis();
		Long timeStart = (Long)getInstance().times.get(key);
		long timeUsed =0;
		if(timeStart!=null){
			timeUsed = timeStop - timeStart.longValue();
		}
		logger.debug("Profiled: "+key+"="+timeUsed);
	}
  
}
