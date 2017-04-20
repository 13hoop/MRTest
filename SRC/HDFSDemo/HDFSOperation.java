package com.hadoop.hdfs.test;

import java.io.IOException;
import java.net.URISyntaxException;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import sun.org.mozilla.javascript.internal.ast.LetNode;

// 几种常见HDFS的操作
public class HDFSOperation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static String uriStr = "hdfs://yongren:9000";
	public static FileSystem fsInstance(String...uriStr) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		if (uriStr[0] != null) {
			URI uri = new URI(uriStr[0]);
			return FileSystem.get(uri, configuration);
		}
		return FileSystem.get(configuration);
	}
	public static void mkdir(String pathStr) throws IOException, URISyntaxException {
		FileSystem fs = fsInstance(uriStr);
		fs.mkdirs(new Path(pathStr));
		fs.close();
	}
	public static void rmdir(String pathStr) throws IOException, URISyntaxException {
		FileSystem fs = fsInstance(uriStr);
		fs.delete(new Path(pathStr), true);
		fs.close();
	}
	public static void lsAll(String pathStr) throws IOException, URISyntaxException {
		FileSystem fs = fsInstance(uriStr);
		FileStatus[] statuses = fs.listStatus(new Path(pathStr));
		Path[] pathes = FileUtil.stat2Paths(statuses);
		for(Path path : pathes) {
			System.out.println(path);
		}
		fs.close();
	}

	// put / get
	public static void putFile(String souStr, String dsStr) throws IOException, URISyntaxException {
		FileSystem fs = fsInstance(uriStr);
		fs.copyFromLocalFile(new Path(souStr),new Path(dsStr));
		fs.close();
	}
	public static void getFile(String souStr, String dsStr) throws IOException, URISyntaxException {
		FileSystem fs = fsInstance(uriStr);
		fs.copyToLocalFile(new Path(souStr),new Path(dsStr));
		fs.close();
	}
}
