package com.qiaoda.fdfs.fastLoadData;

import org.springframework.context.annotation.Scope;

@Scope("prototype") 
public class RedisPoolBean {
	
	private String filename;
	private String from_id;
	private String fn2;
	private String user_id;
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getFrom_id() {
		return from_id;
	}
	public void setFrom_id(String from_id) {
		this.from_id = from_id;
	}
	public String getFn2() {
		return fn2;
	}
	public void setFn2(String fn2) {
		this.fn2 = fn2;
	}
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	
	
}
