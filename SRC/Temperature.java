package com.yongren.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Temperature extends Configured implements Tool {

	// mapper
	public static class TemperatureMap extends Mapper<LongWritable, Text, Text, IntWritable> {		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value
			String str = value.toString();
			int temperature = Integer.parseInt( ((String) str.substring(14, 19)).trim() );
			
			if(temperature != -9999) {
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				// key
				//				String stationId = (String) fileSplit.getPath().getName().substring(5, 10);
				String stationId = "03103";
				context.write(new Text(stationId), new IntWritable(temperature));
			}
		}
	}
	
	// reducer
	public static class TemperatureReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable resule = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for(IntWritable value: values) {
				sum += value.get();
				count++;
			}
			resule.set(sum / count);
			context.write(key, resule);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] args0 = {
				"hdfs://yongren:9000/Temperature/30yr_03103.dat",
				"hdfs://yongren:9000/Temperature/out"
		};
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		System.exit(ec);
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration config = new Configuration();
		
		Path myPath = new Path(arg0[1]);
		FileSystem hdfs = myPath.getFileSystem(config);
		if(hdfs.isDirectory(myPath)) {
			hdfs.delete(myPath, true);
		}
		
		Job job = new Job(config, "Temperature");
		job.setJarByClass(Temperature.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// 输出路径

		job.setMapperClass(TemperatureMap.class);
		job.setReducerClass(TemperatureReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}
}
