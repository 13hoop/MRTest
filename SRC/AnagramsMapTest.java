package com.yongren.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class AnagramsMapTest {
	private Mapper mapper;
	private MapDriver driver;
	
	@Before
	public void init() {
		mapper = new Anagrams.AnagramsMapper();
		driver = new MapDriver(mapper);
	}
	
	@Test
	public void test() {
		String str = "abandonedly";
		driver.withInput(new Object(), new Text(str))
			.withOutput(new Text("aabddelnnoy"), new Text(str))
			.runTest();
	}
}
