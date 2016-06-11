package com.kibou.zk.coordinate;

import java.util.concurrent.TimeUnit;

import com.kibou.zk.ex.BrokenDistributedBarrierException;

public interface DistributedBarrier {
	public int await() 
			throws InterruptedException, BrokenDistributedBarrierException; //TimeountException 
	public int await(long timeount,TimeUnit unit) 
			throws InterruptedException, BrokenDistributedBarrierException;
	
	public void destroy();
}
