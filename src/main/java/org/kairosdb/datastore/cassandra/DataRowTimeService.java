package org.kairosdb.datastore.cassandra;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import org.joda.time.Duration;
import org.kairosdb.core.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataRowTimeService
{
	public static final Logger log = LoggerFactory.getLogger(DataRowTimeService.class);
	private final long storageResolutionMillis;
	private final long rowNumDataPoints;
	
	private final long rowDurationMillis; // product of the above
	
	
	/**
	 * Storage resolution of 1 second, with 300 days in a row
	 * @return
	 */
	public static DataRowTimeService withOneSecondResolution() {
		return new DataRowTimeService(Duration.standardSeconds(1).getMillis(), 
									Duration.standardDays(300).getStandardSeconds() );
	}
	
	/**
	 * storage resolution of 1 millisecond, with 3 weeks in a row
	 * @return
	 */
	public static DataRowTimeService withOneMillisecondResolution() {
		return new DataRowTimeService(Duration.millis(1).getMillis(), 
									Duration.standardDays(21).getMillis() );
	}
	
	/**
	 * 
	 * @param storageResolutionMillis default is 1, can be 1000 for 1sec resolution or anything else. 
	 * @param rowNumDataPoints the number of data points to fit into a wide row. must be < 2Bn.
	 */
	DataRowTimeService(Long storageResolutionMillis, Long rowNumDataPoints) 
	{
		Objects.requireNonNull(storageResolutionMillis);
		Objects.requireNonNull(rowNumDataPoints);
		
		if(storageResolutionMillis < 1) {
			throw new IllegalArgumentException("storage resolution must be positive");
		}
		
		if(rowNumDataPoints < 1) {
			throw new IllegalArgumentException("row width must be positive");
		}
		
		if(rowNumDataPoints > 2000000000) {
			throw new IllegalArgumentException("maximum number of row data points cannot exeed 2Billion (cassandra row limit)");
		}
		
		this.rowNumDataPoints = rowNumDataPoints;
		this.storageResolutionMillis = storageResolutionMillis;
		
		this.rowDurationMillis = this.rowNumDataPoints * this.storageResolutionMillis;
		
		log.info("Creating DataRowTimeService for user data with resolution of {}ms and using {} points per row", this.storageResolutionMillis, this.rowNumDataPoints);
	}
	
	public long getRowNumDataPoints() {
		return this.rowNumDataPoints;
	}
	
	public long getRowDurationMillis() {
		return this.rowDurationMillis;
	}
	
	public long calculateRowTimeStart(DataPoint dataPoint)
	{
		// dataPoints are always in millis
		return calculateRowTimeStart(dataPoint.getTimestamp());
	}
	
	/**
	 * @param timestamp milliseconds epoch timestamp
	 * @param unit
	 * @return the rowTimeStamp in the resolution for the configured storage system 
	 * 
	 * TODO optimise this, it's likely millis throughout.
	 */
	public long calculateRowTimeStart(long timestampMillis)
	{
		return timestampMillis - Math.abs(timestampMillis) % this.rowDurationMillis;
	}
	
		
	public long getRowTimeNow()
	{
		return calculateRowTimeStart(System.currentTimeMillis());
	}
	

	public int getColumnTimeOffset(long rowTimestampStartMillis, long dataTimestampMillis)
	{
		long millis_from_base = dataTimestampMillis - rowTimestampStartMillis;
		
		final int num_of_stor_res_units = new BigDecimal(millis_from_base)
				                       .divide(new BigDecimal(this.storageResolutionMillis), RoundingMode.FLOOR)
				                       .intValue();
		
		/*
			The timestamp is shifted to support legacy datapoints that
			used the extra bit to determine if the value was long or double
		 */
		return (num_of_stor_res_units << 1);
	}

	public long getColumnRealTimestamp(long rowTime, int columnOffset)
	{
		// rowTime is in millis.
		// columnOffset is in units of storage resolution.
		return (rowTime + (long) ( (columnOffset >>> 1) * this.storageResolutionMillis) );
	}

	public static boolean isLongValue(int columnName)
	{
		return ((columnName & 0x1) == CassandraDatastore.LONG_FLAG);
	}
	
}
