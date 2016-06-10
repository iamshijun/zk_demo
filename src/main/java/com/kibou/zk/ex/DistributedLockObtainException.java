package com.kibou.zk.ex;

public class DistributedLockObtainException extends RuntimeException {

	private static final long serialVersionUID = 5373446237986728836L;
	
	public DistributedLockObtainException(){
		super();
	}

	public DistributedLockObtainException(Throwable cause){
		super(cause);
	}
	
	public DistributedLockObtainException(String msg,Throwable cause){
		super(msg, cause);
	}
}
