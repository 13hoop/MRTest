package com.hadoop.hdfs.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
/*
 * 分析：
 * 	源文件是以日期为文件夹的小文件合集，故首先要完成一个文件夹下小文件的合并和上传
 *  然后，在按上述过程把各个文件夹都过一遍
 * */
public class HDFSDemo {
	private static FileSystem local = null;
	private static FileSystem fs = null;
	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		list();
	}
		
	// glob and put
	public static void list() throws  IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		URI uri = new URI("hdfs://yongren:9000");
		fs = FileSystem.get(uri, configuration);
		local = FileSystem.getLocal(configuration);
		
		FileStatus[] statuses = local.globStatus(new Path("/Users/YongRen/Desktop/HdfsTestData/73/*"), new RegexExcludePathFilter("^.*svn$"));
		Path[] dirs = FileUtil.stat2Paths(statuses);
		
		FSDataInputStream inStream = null;
		FSDataOutputStream outStream = null;
		
		for(Path dir: dirs) {
			// 拿到目录名替换为文件名
			String fileName = dir.getName().replace("-", "");
			FileStatus[] fileStatus = local.globStatus(new Path(dir + "/*"), new RegexAcceptPathFilter("^.*txt$"));
			Path[] filePathes = FileUtil.stat2Paths(fileStatus);
			Path fileOutPath = new Path("hdfs://yongren:9000/tempMergeMutiFiles/TVData/" + fileName + ".txt");
			
			outStream = fs.create(fileOutPath);
			for(Path path: filePathes) {
				inStream = local.open(path);
				IOUtils.copyBytes(inStream, outStream, 4096, false);
				inStream.close();
			}
			if(outStream != null) {
				outStream.close();
			}
		}
	}

	// filter：根据题意去除svn等其他文件
	public static class RegexExcludePathFilter implements PathFilter {
		private final String regex;
		public RegexExcludePathFilter(String regex) {
			this.regex = regex;
		}
		
		@Override
		public boolean accept(Path arg0) {
			boolean flag = arg0.toString().matches(regex);
			return !flag;
		}
		
	}
	// filter: 筛选txt文件
	public static class RegexAcceptPathFilter implements PathFilter {
		private final String regex;
		public RegexAcceptPathFilter(String regex) {
			this.regex = regex;
		}
		
		@Override
		public boolean accept(Path arg0) {
			// TODO Auto-generated method stub
			boolean flag = arg0.toString().matches(regex);
			return flag;
		}
		
	}
}
