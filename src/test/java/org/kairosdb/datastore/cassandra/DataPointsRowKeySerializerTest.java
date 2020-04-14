package org.kairosdb.datastore.cassandra;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DataPointsRowKeySerializerTest
{
	public static final Charset UTF8 = Charset.forName("UTF-8");
	
	@Test
	public void test_deserialiseRowKey() throws DecoderException
	{
		ByteBuffer rowKeyBytes = ByteBuffer.wrap(Hex.decodeHex("702e3764343965356633322e393832313764663638642e6d65616e696e67000000004dbafca000000b6b6169726f735f6c6f6e676163633d31323334353a7a6f6e653d313a".toCharArray()));
		
		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();

		DataPointsRowKey rowKey = serializer.fromByteBuffer(rowKeyBytes, "default");

		assertThat(rowKey.getMetricName(), equalTo("p.7d49e5f32.98217df68d.meaning"));
		assertThat(rowKey.getDataType(), equalTo(LongDataPointFactoryImpl.DST_LONG));
		assertThat(rowKey.getTimestamp(), equalTo(LocalDateTime.of(1980, 7, 31, 0, 0, 0).atZone(ZoneId.of("UTC")).toEpochSecond()*1000));
	
	}

	@Test
	public void test_toByteBuffer_oldFormat()
	{
		String metricName = "my.gnarly.metric";
		long now = System.currentTimeMillis();
		//Build old row key buffer
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		buffer.put(metricName.getBytes(UTF8)); //Metric name is put in this way for sorting purposes
		buffer.put((byte)0x0);
		buffer.putLong(now);
		buffer.put("host=myhost:".getBytes(UTF8));

		buffer.flip();

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();

		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer, "default");

		assertThat(rowKey.getMetricName(), equalTo(metricName));
		assertThat(rowKey.getDataType(), equalTo(LegacyDataPointFactory.DATASTORE_TYPE));
		assertThat(rowKey.getTimestamp(), equalTo(now));
	}

	@Test
	public void test_toByteBuffer_legacyType()
	{
		SortedMap<String, String> map = new TreeMap<>();
		map.put("a", "b");

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();
		ByteBuffer buffer = serializer.toByteBuffer(new DataPointsRowKey("myMetric",
				"default", 12345L, LegacyDataPointFactory.DATASTORE_TYPE, map));

		assertThat(buffer.remaining(), equalTo(21));//This should be the size of the legacy buffer
		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer, "default");

		assertThat(rowKey.getMetricName(), equalTo("myMetric"));
		assertThat(rowKey.getDataType(), equalTo(LegacyDataPointFactory.DATASTORE_TYPE));
		assertThat(rowKey.getTimestamp(), equalTo(12345L));
	}

	@Test
	public void test_toByteBuffer_newFormat()
	{
		SortedMap<String, String> map = new TreeMap<>();
		map.put("a", "b");
		map.put("c", "d");
		map.put("e", "f");

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();
		ByteBuffer buffer = serializer.toByteBuffer(new DataPointsRowKey("myMetric",
				"default", 12345L, "myDataType", map));

		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer, "default");

		assertThat(rowKey.getMetricName(), equalTo("myMetric"));
		assertThat(rowKey.getDataType(), equalTo("myDataType"));
		assertThat(rowKey.getTimestamp(), equalTo(12345L));
		assertThat(rowKey.getTags().size(), equalTo(3));
		assertThat(rowKey.getTags().get("a"), equalTo("b"));
		assertThat(rowKey.getTags().get("c"), equalTo("d"));
		assertThat(rowKey.getTags().get("e"), equalTo("f"));

	}

	@Test
	public void test_toByteBuffer_tagsWithColonEquals()
	{
		SortedMap<String, String> map = new TreeMap<>();
		map.put("a:a", "b:b");
		map.put("c=c", "d=d");
		map.put(":e", "f\\");
		map.put("=a=", "===");
		map.put(":a:", ":::");
		map.put("=b=", ":::");
		map.put(":b:", "===");
		map.put("=c=", "normal");

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();
		ByteBuffer buffer = serializer.toByteBuffer(new DataPointsRowKey("myMetric", "default",
				12345L, "myDataType", map));

		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer, "default");

		assertThat(rowKey.getMetricName(), equalTo("myMetric"));
		assertThat(rowKey.getDataType(), equalTo("myDataType"));
		assertThat(rowKey.getTimestamp(), equalTo(12345L));
		assertThat(rowKey.getTags().size(), equalTo(8));
		assertThat(rowKey.getTags().get("a:a"), equalTo("b:b"));
		assertThat(rowKey.getTags().get("c=c"), equalTo("d=d"));
		assertThat(rowKey.getTags().get(":e"), equalTo("f\\"));
		assertThat(rowKey.getTags().get("=a="), equalTo("==="));
		assertThat(rowKey.getTags().get(":a:"), equalTo(":::"));
		assertThat(rowKey.getTags().get("=b="), equalTo(":::"));
		assertThat(rowKey.getTags().get(":b:"), equalTo("==="));
		assertThat(rowKey.getTags().get("=c="), equalTo("normal"));
	}
}