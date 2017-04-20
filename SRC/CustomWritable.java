package com.yongren.MapReduce.InputOutputTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/*自定义数据类型 【播放量 收藏数 评论数 踩数 赞数】封装为一种数据类型
 * 
 * 源数据格式参考：电视剧名 网站代号 【播放量 收藏数 评论数 踩数 赞数】
 * 
 * */ 
public class CustomWritable implements WritableComparable<Object>{

	private int playedNum;
	private int savedNum;
	private int commendedNum;
	private int dislikedNum;
	private int likedNum;
	
	// 构造
	public CustomWritable() {}
	public CustomWritable(int p, int s, int c, int d, int l) {
		this.playedNum = p;
		this.savedNum = s;
		this.commendedNum = c;
		this.dislikedNum = d;
		this.likedNum = l;
	}
	
	// setter
	public void set(int p, int s, int c, int d, int l) {
		this.playedNum = p;
		this.savedNum = s;
		this.commendedNum = c;
		this.dislikedNum = d;
		this.likedNum = l;		
	}
	// getter
	public int getplayedNum() {
		return this.playedNum;
	}
	public int getsavedNum() {
		return this.savedNum;
	}
	public int getcommendedNum() {
		return this.commendedNum;
	}
	public int getdislikedNum() {
		return this.dislikedNum;
	}
	public int getlikedNum() {
		return this.likedNum;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		playedNum = in.readInt();
		savedNum = in.readInt();
		commendedNum = in.readInt();
		dislikedNum = in.readInt();
		likedNum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(playedNum);
		out.writeInt(savedNum);
		out.writeInt(commendedNum);
		out.writeInt(dislikedNum);
		out.writeInt(likedNum);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
