package com.yongren.TVDataDemo.Test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/*
 * 自定义视屏输入的数据：
 * 
 * Text->【name	代号】: TVWriabel->【playedNum, savedNum, commendedNum, dislikeNum, likeNum】
 * 
 * 思路：
 * 	通过自定义TVRecordReader类，借助LineReader类能读取到源文件每一行的数据，按照`\t`分割为字符串数组，
 * 组合前两位为text作为key，后5位为tvWritable作为value
 * 
 * */
public class TVDataInputFormat extends FileInputFormat<Text, TVWritable> {

	@Override
	public RecordReader<Text, TVWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new TVRecordReader();
	}

	public class TVRecordReader extends RecordReader<Text, TVWritable> {

		public LineReader lineReader;
		public Text lineData;
		public Text customKey;
		public TVWritable customValue;
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return customKey;
		}

		@Override
		public TVWritable getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return customValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			// 1 获取文件系统
			Configuration configuration = arg1.getConfiguration();
			FileSplit fileSplit = (FileSplit) arg0;
			Path filePath = fileSplit.getPath(); 
			FileSystem fileSystem = filePath.getFileSystem(configuration);
			
			// 2 init
			FSDataInputStream inputStream = fileSystem.open(filePath);
			lineReader = new LineReader(inputStream, configuration);
			lineData = new Text();
			customKey = new Text();
			customValue = new TVWritable();
		}

		// 每行的数据处理
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int lineSize = lineReader.readLine(lineData);
			if (lineSize == 0) return false;
			
			//1 行数据以‘\t’分割转化为字符串数组
			String[] lineStrArr = lineData.toString().split("\t");
			if(lineStrArr.length != 7) {
				throw new IOException("😯无效的源文件格式： 要求源文件以`\t`划分且数据为7项");
			}
			
			//2 组合
			customKey.set(lineStrArr[0] + "\t" + lineStrArr[1]);
			customValue.set(Integer.parseInt(lineStrArr[2]), Integer.parseInt(lineStrArr[3]), Integer.parseInt(lineStrArr[4]), Integer.parseInt(lineStrArr[5]), Integer.parseInt(lineStrArr[6]));
			return true;
		}
		
	}
}

