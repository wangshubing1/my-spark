package com.king.spark.core.upgrade.applog;

import java.io.Serializable;

/**
 * 访问日志信息类（可序列化）
 * @author Administrator
 *
 */
public class AccessLogInfo implements Serializable {

	private static final long serialVersionUID = 5749943279909593929L;
	
	private long timestamp;		// 时间戳
	private long upTraffic;		// 上行流量
	private long downTraffic;	// 下行流量
	
	public AccessLogInfo() {}
	
	public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
		this.timestamp = timestamp;
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public long getUpTraffic() {
		return upTraffic;
	}
	public void setUpTraffic(long upTraffic) {
		this.upTraffic = upTraffic;
	}
	public long getDownTraffic() {
		return downTraffic;
	}
	public void setDownTraffic(long downTraffic) {
		this.downTraffic = downTraffic;
	}
	
}
