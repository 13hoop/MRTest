package com.yongren.hadoop.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class AnagramsReduceTest {
	private Reducer reducer;
	private ReduceDriver driver;

	@Before
	public void init() {
		reducer = new Anagrams.AnagramsReducer();
		driver = new ReduceDriver(reducer);
	}
	
	@Test
	public void test() {
		// aaacssv	cassava,casavas,

		List values = new ArrayList();
		values.add(new Text("cassava"));
		values.add(new Text("casavas"));
		driver.withInput(new Text("aaacssv"), values)
		      .withOutput(new Text("aaacssv"), new Text("cassava,casavas,"))
		      .runTest();
	}
}
