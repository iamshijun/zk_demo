package com.kibou.zk.ex;

public class DistributedLockDestroyedException extends RuntimeException {

	private static final long serialVersionUID = 5373446237986728836L;
	
	public DistributedLockDestroyedException(){
		super();
	}
	
	public DistributedLockDestroyedException(String msg){
		super(msg);
	}

	public DistributedLockDestroyedException(Throwable cause){
		super(cause);
	}
	
	public DistributedLockDestroyedException(String msg,Throwable cause){
		super(msg, cause);
	}
}
