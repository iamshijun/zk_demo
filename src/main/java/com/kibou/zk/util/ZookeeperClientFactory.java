package com.kibou.zk.util;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClientFactory {

	public static ZooKeeper newZooKeeper(String connectString,int sessionTimeout)
			throws IOException{
		return new ZooKeeper(connectString, sessionTimeout, null, false);
	}
	
	public static ZooKeeper newZooKeeper(String connectString,int sessionTimeout,Watcher watcher)
			throws IOException{
		return new ZooKeeper(connectString, sessionTimeout, watcher, false);
	}
	
	public static ZooKeeper newZooKeeper(String connectString,int sessionTimeout,Watcher watcher,boolean canBeReadOnly)
			throws IOException{
		return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
	}
}
