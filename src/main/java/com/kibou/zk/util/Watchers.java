package com.kibou.zk.util;

import org.apache.zookeeper.Watcher;

public abstract class Watchers{
	
	public final static Watcher DUMMY = DummyWatcher.INSTACE;
	
	private enum DummyWatcher implements Watcher{
		INSTACE;
		
		public void process(org.apache.zookeeper.WatchedEvent event) {};
	}
}


