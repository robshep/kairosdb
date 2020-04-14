package org.kairosdb.datastore.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.base.Stopwatch;


public class SimpleRowTimeServiceTest 
{
	@Test
	public void testRowTimeService_inputProperties()
	{
		// 1ms version
		DataRowTimeService RTS_1milli = DataRowTimeService.withOneMillisecondResolution();
		assertThat(RTS_1milli.getRowDurationMillis()).isEqualTo(1814400000);
		long row_time_start = RTS_1milli.calculateRowTimeStart(1586822553000L);
		assertThat(row_time_start).isEqualTo(1585785600000L);
		int column_time_offset = RTS_1milli.getColumnTimeOffset(row_time_start, 1586822553000L);
		assertThat(column_time_offset).isEqualTo(2073906000);
		
		
		// 25ms version
		DataRowTimeService RTS_25milli = new DataRowTimeService(25L, 1728000000L);	
		assertThat(RTS_25milli.getRowDurationMillis()).isEqualTo(1728000000L * 25L);
		long row_time_start_25 = RTS_25milli.calculateRowTimeStart(1586822553000L);
		assertThat(row_time_start_25).isEqualTo(1555200000000L);
		// millis_offset: 31,622,553,000
		int column_time_offset_25 = RTS_25milli.getColumnTimeOffset(row_time_start_25, 1586822553000L);
		assertThat(column_time_offset_25).isEqualTo(-1765163056);
		assertThat(RTS_25milli.getColumnRealTimestamp(row_time_start_25, column_time_offset_25)).isEqualTo(1586822553000L);
		
		/*
		Stopwatch stopwatch = Stopwatch.createStarted();
		for(long i=0;i<1728000000;i+=100) {
			long j=25L*i;
			int coff = RTS_25milli.getColumnTimeOffset(0, j);
			System.out.println("i=" + i + ", j=" + j + ", coff=" + coff);
			assertThat(RTS_25milli.getColumnRealTimestamp(0, coff)).isEqualTo(j);
		}
		System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS)/1728000000.0);
		*/
	}
}
