/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FilterAggregatorTest
{
	@Test
	public void test_LessThanFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_lt_values");
		group.addDataPoint(new LongDataPoint(1, -10));
		group.addDataPoint(new LongDataPoint(2, -20));
		group.addDataPoint(new LongDataPoint(3, 30));
		group.addDataPoint(new LongDataPoint(4, 40));
		group.addDataPoint(new LongDataPoint(5, -50));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.LT);
		filterAggregator.setThreshold(0.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getLongValue(), equalTo(30L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getLongValue(), equalTo(40L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_LessThanFilterAllNegative()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_lt_all_neg_values");
		group.addDataPoint(new LongDataPoint(1, -10));
		group.addDataPoint(new LongDataPoint(2, -20));
		group.addDataPoint(new LongDataPoint(3, -30));
		group.addDataPoint(new LongDataPoint(4, -40));
		group.addDataPoint(new LongDataPoint(5, -50));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.LT);
		filterAggregator.setThreshold(0.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_LessThanEqualToFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_lte_values");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 15));
		group.addDataPoint(new LongDataPoint(4, 30));
		group.addDataPoint(new LongDataPoint(5, 10));
		group.addDataPoint(new LongDataPoint(6, 25));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.LTE);
		filterAggregator.setThreshold(15.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getLongValue(), equalTo(30L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(6L));
		assertThat(dp.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_GreaterThanFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_gt_values");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 15));
		group.addDataPoint(new LongDataPoint(4, 30));
		group.addDataPoint(new LongDataPoint(5, 10));
		group.addDataPoint(new LongDataPoint(6, 25));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GT);
		filterAggregator.setThreshold(20.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getLongValue(), equalTo(15L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_GreaterThanEqualToFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_gte_values");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 15));
		group.addDataPoint(new LongDataPoint(4, 30));
		group.addDataPoint(new LongDataPoint(5, 10));
		group.addDataPoint(new LongDataPoint(6, 25));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GTE);
		filterAggregator.setThreshold(20.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getLongValue(), equalTo(15L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(false));
        }

	@Test
	public void test_EqualToFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_eq_values");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 15));
		group.addDataPoint(new LongDataPoint(4, 30));
		group.addDataPoint(new LongDataPoint(5, 10));
		group.addDataPoint(new LongDataPoint(6, 25));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.EQUAL);
		filterAggregator.setThreshold(10.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getLongValue(), equalTo(15L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getLongValue(), equalTo(30L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(6L));
		assertThat(dp.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_NonEqualToFilter()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_ne_values");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 15));
		group.addDataPoint(new LongDataPoint(4, 30));
		group.addDataPoint(new LongDataPoint(5, 10));
		group.addDataPoint(new LongDataPoint(6, 25));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.NE);
		filterAggregator.setThreshold(10.0);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_StringFiltering()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_string_values");
		group.addDataPoint(new StringDataPoint(1, "alfa"));
		group.addDataPoint(new StringDataPoint(2, "beta"));
		group.addDataPoint(new StringDataPoint(3, "gamma"));
		group.addDataPoint(new StringDataPoint(4, "delta"));
		group.addDataPoint(new StringDataPoint(5, "alfa"));
		group.addDataPoint(new StringDataPoint(6, "beta"));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.EQUAL);
		filterAggregator.setThreshold("alfa");
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("beta"));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("gamma"));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("delta"));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();assertThat(dp.getTimestamp(), equalTo(6L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("beta"));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_FilteringMixedTypes()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_mixed_types_values");
		group.addDataPoint(new StringDataPoint(1, "alfa"));
		group.addDataPoint(new StringDataPoint(2, "beta"));
		group.addDataPoint(new LongDataPoint(3, 10L));
		group.addDataPoint(new StringDataPoint(4, "delta"));
		group.addDataPoint(new DoubleDataPoint(5, 20.0));
		group.addDataPoint(new StringDataPoint(6, "beta"));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GTE);
		filterAggregator.setThreshold(13.43543);
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(10.0));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_FilteringMixedTypesStringThreshold()
	{
		ListDataPointGroup group = new ListDataPointGroup("test_mixed_types_string_thresh_values");
		group.addDataPoint(new StringDataPoint(1, "alfa"));
		group.addDataPoint(new StringDataPoint(2, "beta"));
		group.addDataPoint(new LongDataPoint(3, 10L));
		group.addDataPoint(new StringDataPoint(4, "delta"));
		group.addDataPoint(new DoubleDataPoint(5, 20.0));
		group.addDataPoint(new StringDataPoint(6, "beta"));

		FilterAggregator filterAggregator = new FilterAggregator();
		filterAggregator.setFilterOp(FilterAggregator.FilterOperation.EQUAL);
		filterAggregator.setThreshold("beta");
		DataPointGroup results = filterAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("alfa"));

		assertThat(results.hasNext(), equalTo(true));
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(((StringDataPoint)dp).getValue(), equalTo("delta"));

		assertThat(results.hasNext(), equalTo(false));
	}

}
