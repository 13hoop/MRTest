package com.yongren.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TemperatureMapTest {
	private Mapper mapper;
	private MapDriver driver;
	
	@Before
	public void init() {
		mapper = new Temperature.TemperatureMap();
		driver = new MapDriver(mapper);
	}
	
	@Test
	public void test() {
		String str ="1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		driver.withInput(new LongWritable(), new Text(str) )
			.withOutput(new Text("03103"), new IntWritable(200))
			.runTest();
	}
}
