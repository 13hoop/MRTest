package com.yongren.hadoop.test;

import java.io.IOException;
import java.util.Arrays;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Anagrams extends Configured implements Tool {

	// mapper: key ～> 字母升序排列 ， value ~> 单词
	public static class AnagramsMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// 获取数据
			String strValue = value.toString();
			if (strValue.length() > 0) {
				char[] strArr = strValue.toCharArray();
				// 排序
				Arrays.sort(strArr);
				// key
				String strKey = new String(strArr); 		
				
				context.write(new Text(strKey), new Text(strValue));
				
			}
			
		}
	}
	// reducer: key ~> 字母升序排列 ， values ~> 单词们
	public static class AnagramsReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String strValue = "";
			int count = 0;
			for (Text word: values) {
				strValue += word.toString() + ",";
				count++;
			}
			if (count > 1) {
				context.write(key, new Text(strValue));
			}			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {
				"hdfs://yongren:9000/Anagrams/anagram.txt",
				"hdfs://yongren:9000/Anagrams/out"
		};
		int status = ToolRunner.run(new Configuration(), new Anagrams(), args0);
		System.exit(status);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = new Configuration();
		
		Path path = new Path(arg0[0]);
		FileSystem hdfs = path.getFileSystem(configuration);
		if (hdfs.isDirectory(path)) {
			hdfs.delete(path,true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Anagrams");
		job.setJarByClass(Anagrams.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setMapperClass(AnagramsMapper.class);
		job.setReducerClass(AnagramsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return 0;
	}

}
