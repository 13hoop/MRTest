package com.yongren.TVDataDemo.Test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 *  处理视屏数据的MapReduce
 *  要求：
 *    根据不同的视屏网站，输出不同的文件；
 *    每个文件中统计视屏被的【播放次数，点赞...】等信息
 *  分析：
 *    1> 自定义类型 + 自定义文件输入
 *    2> Map: {Name 网站编号 ： 自定义组合的类型「播放数，收藏数 ...」}
 *    3> Reduce: 先根据网站编号，得到文件名，格式如： “网站名-part-r”； 在按照文件名输出文件
 *  
 * */
public class TVDataOpetation extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		String[] args0 = {
			"hdfs://yongren:9000/TVDataDemoTest/tvplay.txt",
			"hdfs://yongren:9000/TVDataDemoTest/out"
		};
		int ec = ToolRunner.run(new Configuration(), new TVDataOpetation(), args0);
		System.exit(ec);
	}
	
	// mapper
	public static class TvMap extends Mapper<Text, TVWritable, Text, TVWritable> {
		public void map(Text key, TVWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	// reduce
	public static class TvReduce extends Reducer<Text, TVWritable, Text, Text> {
		private MultipleOutputs<Text,Text> mos;
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		public void reduce(Text key, Iterable<TVWritable> values, Context context) throws IOException, InterruptedException {
			
			// value
			int played = 0;
			int saved = 0;
			int commended = 0;
			int disLiked = 0;
			int liked = 0;
			for(TVWritable value: values) {
				played += value.getplayedNum();
				saved += value.getsavedNum();
				commended += value.getcommendedNum();
				disLiked += value.getdislikedNum();
				liked += value.getlikedNum();
			}
			
			String[] strArr = key.toString().split("\t");
			outKey.set(strArr[0]);
			outValue.set(played + " -- " + saved + " -- " + commended + " -- " + disLiked + " -- " + liked);
			if (strArr[1].equals("1")) {
				mos.write("youku", outKey, outValue);
			}else if(strArr[1].equals("2")) {
				mos.write("souhu", outKey, outValue);
			}else if(strArr[1].equals("3")) {
				mos.write("tudou", outKey, outValue);
			}else if(strArr[1].equals("4")) {
				mos.write("aiqiyi", outKey, outValue);
			}else if(strArr[1].equals("5")) {
				mos.write("xunlei", outKey, outValue);
			}			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

	}
	
	@Override
	public int run(String[] arg0) throws Exception {

		Configuration config = new Configuration();
		
		Path myPath = new Path(arg0[1]);
		FileSystem hdfs = myPath.getFileSystem(config);
		if(hdfs.isDirectory(myPath)) {
			hdfs.delete(myPath, true);
		}
		
		Job job = new Job(config, "TVDataOpetation");
		job.setJarByClass(TVDataOpetation.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// 输出路径
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class, Text.class, Text.class);
		
		job.setMapperClass(TvMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TVWritable.class);
		job.setReducerClass(TvReduce.class);
		
		job.setInputFormatClass(TVDataInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return 0;
	}
}
