package com.kibou.zk.ex;

public class BrokenDistributedBarrierException extends Exception {

	private static final long serialVersionUID = 5373446237986728836L;
	
	public BrokenDistributedBarrierException(){
		super();
	}

	public BrokenDistributedBarrierException(Throwable cause){
		super(cause);
	}
	
	public BrokenDistributedBarrierException(String msg,Throwable cause){
		super(msg, cause);
	}
}
