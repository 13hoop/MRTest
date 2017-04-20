package com.yongren.hadoop.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TemperatureReducerTest {
	private Reducer reducer;
	private ReduceDriver driver;
	
	@Before
	public void init() {
		reducer = new Temperature.TemperatureReduce();
		driver = new ReduceDriver(reducer);
	}
	
	@Test
	public void test() {
		String k = "03103";
		List values = new ArrayList();
		values.add(new IntWritable(200));
		values.add(new IntWritable(400));
		
		driver.withInput(new Text(k), values)
			.withOutput(new Text(k), new IntWritable(300))
			.runTest();
	}
	
}
