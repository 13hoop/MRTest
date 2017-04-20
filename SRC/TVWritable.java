package com.yongren.TVDataDemo.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * 组合视屏数据：
 * 
 * 	【playedNum, savedNum, commendedNum, dislikeNum, likeNum】
 * 
 * */

public class TVWritable implements WritableComparable<Text> {

	private int playedNum;
	private int savedNum;
	private int commendedNum;
	private int dislikedNum;
	private int likedNum;
	
	// 构造
	public TVWritable() {}
	public TVWritable(int p, int s, int c, int d, int l) {
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
	public int compareTo(Text o) {
		// TODO Auto-generated method stub
		return 0;
	}
}

