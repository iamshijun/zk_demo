package com.kibou.zk.ex;

import org.apache.zookeeper.KeeperException;

public abstract class ExceptionHandler {

	public static void handleZkThreadEx(Exception ke){
		if(ke instanceof InterruptedException){
			
		}else if(ke instanceof KeeperException){
			
		}
	}
}
