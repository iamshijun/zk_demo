package com.kibou.zk.coordinate;

import java.util.concurrent.locks.Lock;

public interface DistributedLock extends Lock{

	public boolean destroy();
}
