package com.hadoop.hdfs.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HDFSMutiFiles {

	private static FileSystem local = null;
	private static FileSystem fs = null;
	
	// glob and put
	public static void list(Path dstPath) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI("hdfs://yongren:9000");
		fs = FileSystem.get(uri, configuration);
		local = FileSystem.getLocal(configuration);
		FileStatus[] status = local.globStatus(new Path("/Users/YongRen/Desktop/pathPattern/205_data/data/*"),
				new RegexAcceptPathFilter("^.*txt$"));
		Path[] listedPathes = FileUtil.stat2Paths(status);
		for (Path path: listedPathes) {
			fs.copyFromLocalFile(path, dstPath);
		}
	}
	// filter
	public static class RegexAcceptPathFilter implements PathFilter {
		
		private final String regex;
		public RegexAcceptPathFilter(String regex) {
			// TODO Auto-generated constructor stub
			this.regex = regex;
		}
		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return flag;
		}
		
	}
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		// TODO Auto-generated method stub
		Path path = new Path("hdfs://yongren:9000/tempMergeMutiFiles");
		list(path);
	}

}
