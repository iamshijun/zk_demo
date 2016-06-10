package com.kibou.zk.ex;

public class DistributedBarrierException extends RuntimeException {

	private static final long serialVersionUID = 5373446237986728836L;
	
	public DistributedBarrierException(){
		super();
	}

	public DistributedBarrierException(Throwable cause){
		super(cause);
	}
	
	public DistributedBarrierException(String msg,Throwable cause){
		super(msg, cause);
	}
}
