package org.kairosdb.datastore.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowTimeService
{
	public static final Logger log = LoggerFactory.getLogger(RowTimeService.class);

	public static final DataRowTimeService DEFAULT_3wks1ms = DataRowTimeService.withOneMillisecondResolution();
	
	public final DataRowTimeService userDataRowsTimeService;
	
	public DataRowTimeService forMetric(String metric) {
		if(metric.startsWith("kairosdb.")) {
			return DEFAULT_3wks1ms;
		} else {
			return userDataRowsTimeService;
		}
	}
	
	/**
	 * 
	 * @param storageResolutionMillis default is 1, can be 1000 for 1sec resolution or anything else. 
	 * @param rowNumDataPoints the number of data points to fit into a wide row. must be < 2Bn.
	 */
	RowTimeService(Long storageResolutionMillis, Long rowNumDataPoints)
	{
		this.userDataRowsTimeService = new DataRowTimeService(storageResolutionMillis, rowNumDataPoints);
	}
	
	private RowTimeService() {
		this.userDataRowsTimeService = DEFAULT_3wks1ms;
	}

	public static RowTimeService getDefault() 
	{
		return new RowTimeService();
	}
	
}
