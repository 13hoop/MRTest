package com.yongren.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AnagramsTest {

	private Mapper mapper;
	private Reducer reducer;
	private MapReduceDriver driver;
	
	@Before
	public void init() {
		mapper = new Anagrams.AnagramsMapper();
		reducer = new Anagrams.AnagramsReducer();
		driver = new MapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void test() {
		String str1 ="cassava";
		String str2 = "casavas";
		String k = "aaacssv";
		driver.withInput(new Object(), new Text(str1))
		      .withInput(new Object(), new Text(str2))
			  .withOutput(new Text(k), new Text("cassava,casavas,"))
			  .runTest();
	}

}
