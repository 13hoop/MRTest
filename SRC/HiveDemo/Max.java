package com.yongren.hiveUDFD;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Max extends UDF {
	public Double evaluate(Double a, Double b) {
		if( a == null ) a = 0.0;
		if( b == null ) b = 0.0;
		if( a >= b ) {
			return a;
		}else {
			return b;
		}
	}
}
