package com.kibou.zk.coordinate;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.kibou.zk.ex.BrokenDistributedBarrierException;
import com.kibou.zk.ex.DistributedBarrierException;

public interface DistributedBarrier {
	public int await() 
			throws InterruptedException, BrokenDistributedBarrierException, DistributedBarrierException;
	public int await(long time,TimeUnit unit) 
			throws InterruptedException, BrokenDistributedBarrierException, TimeoutException, DistributedBarrierException;
	
	public void destroy();
}
