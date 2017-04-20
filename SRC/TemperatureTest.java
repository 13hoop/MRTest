package com.yongren.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TemperatureTest {
	private Mapper mapper;
	private Reducer reducer;
	private MapReduceDriver driver;
	
	@Before
	public void init() {
		mapper = new Temperature.TemperatureMap();
		reducer = new Temperature.TemperatureReduce();
		driver = new MapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void test() {
		String str1 ="1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		String str2 = "1985 07 31 11   100    56 -9999    50     5 -9999     0 -9999";
		String k = "03103";
		driver.withInput(new LongWritable(), new Text(str1))
		      .withInput(new LongWritable(), new Text(str2))
			  .withOutput(new Text(k), new IntWritable(150))
			  .runTest();
	}
}
