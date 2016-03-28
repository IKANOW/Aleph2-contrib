/*******************************************************************************
 * Copyright 2012, The Infinit.e Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.v1.document_db.utils;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.bson.types.ObjectId;

import scala.Tuple2;
import scala.Tuple4;

import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.infinit.e.data_model.api.knowledge.AdvancedQueryPojo;
import com.ikanow.infinit.e.data_model.custom.InfiniteMongoSplitter;
import com.ikanow.infinit.e.data_model.store.DbManager;
import com.ikanow.infinit.e.data_model.store.config.source.SourcePojo;
import com.ikanow.infinit.e.data_model.store.document.DocumentPojo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/** A set of c/p'd code from V1
 * NOTE NO TEST COVERAGE FOR THIS SINCE IT IS C/P FROM 
 * BEWARE OF MAKING ANY CHANGES WITHOUT EITHER ADDING TESTS OR THOROUGHLY TESTING BY HAND
 * @author Alex
 */
public class LegacyV1HadoopUtils {

	/** parse the V1 query string 
	 * @param query
	 * @return the required objects embedded in various tuples
	 */
	public static Tuple4<String, Tuple2<Integer, Integer>, BasicDBObject, DBObject> parseQueryObject(final String query, final List<String> community_ids) {
		// Some fixed variables just to avoid changing the guts of the (tested in v1) code
		final boolean isCustomTable = false; 
		@SuppressWarnings("unused")
		Integer nDebugLimit = null;		
		final boolean bLocalMode = false;
		@SuppressWarnings("unused")
		final Boolean incrementalMode = null;
		final String input = "doc_metadata.metadata";

		// Output objects
		final String out_query; 
		int nSplits = 8;
		int nDocsPerSplit = 12500;
		
		List<ObjectId> communityIds = community_ids.stream().map(s -> new ObjectId(s)).collect(Collectors.toList());		

		//C/P code:

		//add communities to query if this is not a custom table
		BasicDBObject oldQueryObj = null;
		BasicDBObject srcTags = null;
		// Start with the old query:
		if (query.startsWith("{")) {
			oldQueryObj = (BasicDBObject) com.mongodb.util.JSON.parse(query);
		}
		else {
			oldQueryObj = new BasicDBObject();
		}
		boolean elasticsearchQuery = oldQueryObj.containsField("qt") && !isCustomTable;
		@SuppressWarnings("unused")
		int nLimit = 0;
		if (oldQueryObj.containsField(":limit")) {
			nLimit = oldQueryObj.getInt(":limit");
			oldQueryObj.remove(":limit");
		}
		if (oldQueryObj.containsField(":splits")) {
			nSplits = oldQueryObj.getInt(":splits");
			oldQueryObj.remove(":splits");
		}
		if (oldQueryObj.containsField(":srctags")) {
			srcTags = new BasicDBObject(SourcePojo.tags_, oldQueryObj.get(":srctags"));
			oldQueryObj.remove(":srctags");
		}
		if (bLocalMode) { // If in local mode, then set this to a large number so we always run inside our limit/split version
			// (since for some reason MongoInputFormat seems to fail on large collections)
			nSplits = InfiniteMongoSplitter.MAX_SPLITS; 
		}
		if (oldQueryObj.containsField(":docsPerSplit")) {
			nDocsPerSplit = oldQueryObj.getInt(":docsPerSplit");
			oldQueryObj.remove(":docsPerSplit");
		}
		final DBObject fields = (DBObject) oldQueryObj.remove(":fields");
		oldQueryObj.remove(":output");
		oldQueryObj.remove(":reducers");
		@SuppressWarnings("unused")
		String mapperKeyClass = oldQueryObj.getString(":mapper_key_class", "");
		@SuppressWarnings("unused")
		String mapperValueClass = oldQueryObj.getString(":mapper_value_class", "");
		oldQueryObj.remove(":mapper_key_class");
		oldQueryObj.remove(":mapper_value_class");
		String cacheList = null;
		Object cacheObj = oldQueryObj.get(":caches");
		if (null != cacheObj) {
			cacheList = cacheObj.toString(); // (either array of strings, or single string)
			if (!cacheList.startsWith("[")) {
				cacheList = "[" + cacheList + "]"; // ("must" now be valid array)
			}
			oldQueryObj.remove(":caches");
		}//TESTED

//		if (null != nDebugLimit) { // (debug mode override)
//			nLimit = nDebugLimit;
//		}
//		boolean tmpIncMode = ( null != incrementalMode) && incrementalMode; 

		@SuppressWarnings("unused")
		String otherCollections = null;
		Date fromOverride = null;
		Date toOverride = null;
		Object fromOverrideObj = oldQueryObj.remove(":tmin");
		Object toOverrideObj = oldQueryObj.remove(":tmax");
		if (null != fromOverrideObj) {
			fromOverride = dateStringFromObject(fromOverrideObj, true);
		}
		if (null != toOverrideObj) {
			toOverride = dateStringFromObject(toOverrideObj, false);
		}

		if ( !isCustomTable )
		{
			if (elasticsearchQuery) {
				oldQueryObj.put("communityIds", communityIds);
				//tmin/tmax not supported - already have that capability as part of the query
			}
			else {
				if (input.equals("feature.temporal")) {
					if ((null != fromOverride) || (null != toOverride)) {
						oldQueryObj.put("value.maxTime", createDateRange(fromOverride, toOverride, true));
					}//TESTED
					oldQueryObj.put("_id.c", new BasicDBObject(DbManager.in_, communityIds));
				}
				else {
					oldQueryObj.put(DocumentPojo.communityId_, new BasicDBObject(DbManager.in_, communityIds));
					if ((null != fromOverride) || (null != toOverride)) {
						oldQueryObj.put(JsonUtils._ID, createDateRange(fromOverride, toOverride, false));
					}//TESTED			
					if (input.equals("doc_metadata.metadata")) {
						oldQueryObj.put(DocumentPojo.index_, new BasicDBObject(DbManager.ne_, "?DEL?")); // (ensures not soft-deleted)
					}
				}
			}
		}
		else
		{
			throw new RuntimeException("Custom Tables not currently supported (no plans to)");
//			if ((null != fromOverride) || (null != toOverride)) {
//				oldQueryObj.put(JsonUtils._ID, createDateRange(fromOverride, toOverride, false));
//			}//TESTED
//			//get the custom table (and database)
//
//			String[] candidateInputs = input.split("\\s*,\\s*");
//			input = CustomOutputManager.getCustomDbAndCollection(candidateInputs[0]);
//			if (candidateInputs.length > 1) {				
//				otherCollections = Arrays.stream(candidateInputs)
//						.skip(1L)
//						.map(i -> CustomOutputManager.getCustomDbAndCollection(i))
//						.map(i -> "mongodb://"+dbserver+"/"+i).collect(Collectors.joining("|"));
//			}
		}		
		out_query = oldQueryObj.toString();

		return Tuples._4T(out_query, Tuples._2T(nSplits, nDocsPerSplit), srcTags, fields);
	}

	/** C/P from v1 InfiniteHadoopUtils.createDateRange
	 * @param min
	 * @param max
	 * @param timeNotOid
	 * @return
	 */
	public static BasicDBObject createDateRange(Date min, Date max, boolean timeNotOid) {
		BasicDBObject toFrom = new BasicDBObject();
		if (null != min) {
			if (timeNotOid) {
				toFrom.put(DbManager.gte_, min.getTime());
			}
			else {
				toFrom.put(DbManager.gte_, new ObjectId(min));

			}
		}
		if (null != max) {
			if (timeNotOid) {
				toFrom.put(DbManager.lte_, max.getTime());
			}
			else {
				toFrom.put(DbManager.lte_, new ObjectId(max));				
			}
		}
		return toFrom;
	}//TESTED (min/max, _id/time)


	/** C/P from v1 InfiniteHadoopUtils.dateStringFromObject
	 * @param o
	 * @param minNotMax
	 * @return
	 */
	public static Date dateStringFromObject(Object o, boolean minNotMax) {
		if (null == o) {
			return null;
		}
		else if (o instanceof Long) {
			return new Date((Long)o);
		}
		else if (o instanceof Integer) {
			return new Date((long)(int)(Integer)o);
		}
		else if (o instanceof Date) {
			return (Date)o;
		}
		else if (o instanceof DBObject) {
			o = ((DBObject) o).get("$date");
		}
		if (o instanceof String) {
			AdvancedQueryPojo.QueryTermPojo.TimeTermPojo time = new AdvancedQueryPojo.QueryTermPojo.TimeTermPojo();
			if (minNotMax) {
				time.min = (String) o;
				Interval i = parseMinMaxDates(time, 0L, new Date().getTime(), false);
				return i.getStart().toDate();
			}
			else {
				time.max = (String) o;
				Interval i = parseMinMaxDates(time, 0L, new Date().getTime(), false);
				return i.getEnd().toDate();				
			}
		}
		else {
			return null;
		}
	}//TESTED: relative string. string, $date obj, number - parse failure + success

	/** Simple isomorphism of joda.Interval to keep as much legacy code unchanged as possible
	 * @author Alex
	 */
	public static class Interval {
		public Interval(long start, long end) {
			_start = new DateElement(new Date(start));
			_end = new DateElement(new Date(end));
		}
		public DateElement getStart() { return _start; }
		public DateElement getEnd() { return _end; }
		
		private final DateElement _start;		
		private final DateElement _end;
		
		public static class DateElement {
			private final Date _d;
			public DateElement(Date d) {
				_d = d;
			}
			public Date toDate() {
				return _d;
			}
		}
	}
	
	/** C/P from v1 QueryHandler.parseMinMaxDates
	 * @param time
	 * @param nMinTime
	 * @param nMaxTime
	 * @param lockMaxToNow
	 * @return
	 */
	public static Interval parseMinMaxDates(AdvancedQueryPojo.QueryTermPojo.TimeTermPojo time, long nMinTime, long nMaxTime, boolean lockMaxToNow) {

		if ((null != time.min) && (time.min.length() > 0)) {
			if (time.min.equals("now")) { 
				nMinTime = nMaxTime;
			}
			else if (time.min.startsWith("now")) { // now+N[hdmy] 
				// now+Xi or now-Xi
				long sgn = 1L;
				if ('-' == time.min.charAt(3)) { //now+ or now-
					sgn = -1L;
				}
				long offset = sgn*getInterval(time.min.substring(4), 'd'); // (default interval is day)
				nMinTime = nMaxTime + offset; // (maxtime is now)
			}
			else if (time.min.equals("midnight")) { 
				nMinTime = DateUtils.truncate(new Date(nMaxTime), Calendar.DAY_OF_MONTH).getTime();
			}
			else if (time.min.startsWith("midnight")) { // midnight+N[hdmy] 
				// midnight+Xi or midnight-Xi
				long sgn = 1L;
				if ('-' == time.min.charAt(8)) { //now+ or now-
					sgn = -1L;
				}
				long offset = sgn*getInterval(time.min.substring(9), 'd'); // (default interval is day)
				nMinTime = DateUtils.truncate(new Date(nMaxTime), Calendar.DAY_OF_MONTH).getTime() + offset; // (maxtime is now)
			}
			else {
				try {
					nMinTime = Long.parseLong(time.min); // (unix time format)
					if (nMinTime <= 99999999) { // More likely to be YYYYMMDD
						// OK try a bunch of common date parsing formats
						nMinTime = parseDate(time.min);							
					} // TESTED for nMaxTime
				}
				catch (NumberFormatException e) {
					// OK try a bunch of common date parsing formats
					nMinTime = parseDate(time.min);
				}
			}
		}
		if ((null != time.max) && (time.max.length() > 0)) {
			if (time.max.equals("midnight")) { 
				nMaxTime = DateUtils.truncate(new Date(nMaxTime), Calendar.DAY_OF_MONTH).getTime();
			}
			else if (time.max.startsWith("midnight")) { // midnight+N[hdmy] 
				// midnight+Xi or midnight-Xi
				long sgn = 1L;
				if ('-' == time.max.charAt(8)) { //now+ or now-
					sgn = -1L;
				}
				long offset = sgn*getInterval(time.min.substring(9), 'd'); // (default interval is day)
				nMaxTime = DateUtils.truncate(new Date(nMaxTime), Calendar.DAY_OF_MONTH).getTime() + offset; // (maxtime is now)
			}
			else if (!time.max.equals("now")) { // (What we have by default)
				if (time.max.startsWith("now")) { // now+N[hdmy] 
					// now+Xi or now-Xi
					long sgn = 1L;
					if ('-' == time.max.charAt(3)) { //now+ or now-
						sgn = -1L;
					}
					long offset = sgn*getInterval(time.max.substring(4), 'd'); // (default interval is day)
					nMaxTime = nMaxTime + offset; // (maxtime is now)
				}
				else {
					try {
						nMaxTime = Long.parseLong(time.max); // (unix time format)
						if (nMaxTime <= 99999999) { // More likely to be YYYYMMDD
							// OK try a bunch of common date parsing formats
							nMaxTime = parseDate(time.max);

							// max time, so should be 24h-1s ahead ...
							nMaxTime = nMaxTime - (nMaxTime % (24*3600*1000));
							nMaxTime += 24*3600*1000 - 1;										

						} //TESTED (logic5, logic10 for maxtime)
					}
					catch (NumberFormatException e) {
						// OK try a bunch of common date parsing formats
						nMaxTime = parseDate(time.max);

						// If day only is specified, should be the entire day...
						if (!time.max.contains(":")) {
							nMaxTime = nMaxTime - (nMaxTime % (24*3600*1000));
							nMaxTime += 24*3600*1000 - 1;							
						}
					}//TOTEST max time
				}
			}				
		} //TESTED (logic5)


		if (lockMaxToNow) {
			if ((null == time.max) || time.max.isEmpty()) {
				nMinTime = new Date().getTime();
				nMaxTime = Long.MAX_VALUE;
			}
			else { // set max to now
				long now = new Date().getTime();
				if ((null != time.min) && !time.min.isEmpty()) { // set min also
					nMinTime = nMinTime + (now - nMaxTime);
				}
				nMaxTime = now;
			}
		}//TESTED (by hand)
		if (nMinTime > nMaxTime) { //swap these round if not ordered correctly
			long tmp = nMinTime;
			nMinTime = nMaxTime;
			nMaxTime = tmp;
		}//TESTED (by hand)

		return new Interval(nMinTime, nMaxTime);		
	}

	/** C/P from v1 QueryHandler.getInterval
	 * @param interval
	 * @param defaultInterval
	 * @return
	 */
	public static long getInterval(String interval, char defaultInterval) {

		if (interval.equals("month")) { // Special case
			return 30L*24L*3600L*1000L;
		}

		int nLastIndex = interval.length() - 1;
		long nDecayTime;
		char c = interval.charAt(nLastIndex);
		if (c >= 0x40) { // it's a digit, interpret:
			nDecayTime = Long.parseLong(interval.substring(0, nLastIndex));
		}
		else { // No digit use default
			c = defaultInterval;
			nDecayTime = Long.parseLong(interval);
		}
		if ('h' == c) {
			nDecayTime *= 3600L*1000L;
		}
		else if ('d' == c) {
			nDecayTime *= 24L*3600L*1000L;
		}
		else if ('w' == c) {
			nDecayTime *= 7L*24L*3600L*1000L;
		}
		else if ('m' == c) {
			nDecayTime *= 30L*24L*3600L*1000L;
		}
		else if ('y' == c) {
			nDecayTime *= 365L*24L*3600L*1000L;
		}
		return nDecayTime;
	}//TESTED

	private static String[] _allowedDatesArray = null;
	// (odd, static initialization doesn't work; just initialize first time in utility fn)

	/** C/P from v1 QueryHandler.getInterval
	 * @param sDate
	 * @return
	 */
	private static long parseDate(String sDate) {
		if (null == _allowedDatesArray) {
			_allowedDatesArray = new String[]
					{
					"yyyy'-'DDD", "yyyy'-'MM'-'dd", "yyyyMMdd", "dd MMM yyyy", "dd MMM yy", 
					"MM/dd/yy", "MM/dd/yyyy", "MM.dd.yy", "MM.dd.yyyy", 
					"yyyy'-'MM'-'dd hh:mm:ss", "dd MMM yy hh:mm:ss", "dd MMM yyyy hh:mm:ss",
					"MM/dd/yy hh:mm:ss", "MM/dd/yyyy hh:mm:ss", "MM.dd.yy hh:mm:ss", "MM.dd.yyyy hh:mm:ss",
					DateFormatUtils.ISO_DATE_FORMAT.getPattern(),
					DateFormatUtils.ISO_DATETIME_FORMAT.getPattern(),
					DateFormatUtils.ISO_DATE_TIME_ZONE_FORMAT.getPattern(),
					DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(),
					DateFormatUtils.SMTP_DATETIME_FORMAT.getPattern()
					};			
		}
		try {
			Date date = DateUtils.parseDate(sDate, _allowedDatesArray);			
			return date.getTime();
		}
		catch (Exception e) { // Error all the way out
			throw new RuntimeException(e);
		}		
	}//TESTED (logic5)

}
