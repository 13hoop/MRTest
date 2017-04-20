package com.yongren.MepReduce.PartitionerDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 分别找出最受欢迎到的男女明显，体会combiner和partitioner在MepReduce处理中的作用
 * */
public class StarDemo extends Configured implements Tool {
	
	// map => [sex : [name	num]]
	public static class StarMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");
			String keyStr = data[1].trim();
			String valueStr = data[0] + "\t" + data[2];
			if(keyStr.length() > 0) {
				context.write(new Text(keyStr), new Text(valueStr));
			}
		}
	}
	
	/*combiner
	 * 对每个map的输入结果进行合并，以便减少磁盘IO和网络IO 
	 *  [sex : [nameMax	numMax]]
	 * */ 
	public static class StarCombiner extends Reducer<Text, Text, Text, Text> {
		private Text text = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int maxSearchNum = Integer.MIN_VALUE;
			int num = 0;
			String name = "";
			for(Text val: values) {
				String[] valueStr = val.toString().split("\t");
				num = Integer.parseInt(valueStr[1]);
				if(num > maxSearchNum) {
					maxSearchNum = num;
					name = valueStr[0];
				}
			}
			text.set(name + "\t" + maxSearchNum);
			context.write(key, text);
		}
	}
	
	/*partitioner
	 * 对于所有结果按照性别进行分区，这了2个性别，就是2个分区，其中0区为male，1区为female
	 * 起输入是map后的结果，输出格式同reduce
	 * */
	public static class StarPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numOfReduceTask) {
			String sex = key.toString().trim();
			if (numOfReduceTask == 0) {
				return 0;
			}
			if(sex.equals("male")) {
				return 0;
			}else if(sex.equals("female")) {
				return 1 % numOfReduceTask;
			}else {
				return 2 % numOfReduceTask;
			}
		}
	}
	
	// reducer: 输出为 【name ： [sex	maxNum]】
	public static class StarReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sex = key.toString();
			String name = "";
			int maxNum = Integer.MIN_VALUE;
			int num = 0;
			for (Text val: values) {
				String[] infoStrs = val.toString().split("\t");
				num = Integer.parseInt(infoStrs[1]);
				if(num > maxNum) {
					maxNum = num;
					name = infoStrs[0];
				}
			}
			context.write(new Text(name), new Text(key + "\t" + maxNum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		String[] argPath = {
				"hdfs://yongren:9000/StarDemo/actor.txt",
				"hdfs://yongren:9000/StarDemo/out"
		};
		int state = ToolRunner.run(new Configuration(), new StarDemo(), argPath);
		System.exit(state);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = new Configuration();
		Path path = new Path(arg0[1]);
		FileSystem haFileSystem = path.getFileSystem(configuration);
		if (haFileSystem.isDirectory(path)) {
			haFileSystem.delete(path, true);
		}
		
		Job job = new Job(configuration, "StarDemo");
		job.setJarByClass(StarDemo.class);
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(StarPartitioner.class);
		
		job.setMapperClass(StarMap.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setCombinerClass(StarCombiner.class);
		
		job.setReducerClass(StarReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.waitForCompletion(true);
		return 0;
	}
}
