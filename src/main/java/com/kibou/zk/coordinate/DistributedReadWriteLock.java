package com.kibou.zk.coordinate;

import java.util.concurrent.locks.ReadWriteLock;

public interface DistributedReadWriteLock extends ReadWriteLock{

	DistributedLock readLock();
	
	DistributedLock writeLock();
}
