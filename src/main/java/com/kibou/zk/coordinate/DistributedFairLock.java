package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.ex.DistributedLockDestroyedException;
import com.kibou.zk.ex.DistributedLockObtainException;
import com.kibou.zk.util.Watchers;
import com.kibou.zk.util.ZookeeperClientFactory;

//make this Reentrant able
public class DistributedFairLock implements Watcher,VoidCallback, DistributedLock { // lock without herd effect (WEF)
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static String defaultZnode = "/lock/fairLock"; //default
	
	private final ZooKeeper zk;
	private final String znode;
	
	private final String nodeName = "dfl";
	private String nodeNamePrefix;

	private boolean destroy = false;
	
	//cause Zookeeper's thread should see lock thread's  modification
	private volatile String nodeWaitingFor = "";
	private String nodeCreated = ""; // currentNode , use by lock,unlock cannot seen by zk thread
	
	public DistributedFairLock() {
		this(defaultZnode);
	}
	
	public DistributedFairLock(String znode) {
		assert znode != null;
		this.znode = znode;
		this.nodeNamePrefix = znode + "/" + nodeName;
		try {
			zk = ZookeeperClientFactory.newZooKeeper(
					ZookeeperCfgConstants.CONNECTSTRING,
					ZookeeperCfgConstants.SESSION_TIMEOUT,
					this);
		} catch (IOException  e) {
			throw new DistributedLockObtainException(e);
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if(path != null){
			EventType eventType = event.getType();
			if(path.equals(nodeWaitingFor)){
				switch(eventType){
					//case None
					case NodeDeleted: // 判断是否和tryAcquire中得到的path一致
						doNotify(); break;
					default: 
						try {
							zk.exists(nodeWaitingFor, true);
							logger.debug("Got event : " + event.getType());
						} catch (KeeperException | InterruptedException e) {
							doNotify(); 	
							e.printStackTrace();
						}
						break;
				}
			}else if(path.equals(znode)){
				switch (eventType){
					case NodeChildrenChanged:
						doNotify(); break;
					default:
						try {
							zk.exists(znode, true);
							logger.debug("Got event : " + event.getType());
						} catch (KeeperException | InterruptedException e) {
							doNotify(); 	
							e.printStackTrace();
						}
						break;
				}
			}
		}
	}
	
	private void doNotify(){
		synchronized (this) {
			notify();
		}
	}
	
	private int getPathSeq(String path,String prefix){
		String seq = path.substring(prefix.length()+1);
		try{//假设没有恶意或者程序错误 seq 都是可转成数值的
			return Integer.parseInt(seq);
		}catch(NumberFormatException nfe){
			nfe.printStackTrace();//oh no! forget about this!
			return Integer.MAX_VALUE;
		}
	}
	
	private void checkIfDestroyAlready() {
		if(destroy){
			throw new DistributedLockDestroyedException("Lock has been destroyed");
		}
	}
	
	public void lock(){
		checkIfDestroyAlready();
		
		boolean interrupted = false;
		
		synchronized(this){
			while(!destroy){
				try {
					if(!tryAcquire()){
						zk.exists(nodeWaitingFor, true);
						wait();
					}else{
						return;
					}
				} catch (InterruptedException e) {
					interrupted = true;
				} catch (KeeperException e) {
					throw new DistributedLockObtainException(e);
				}
			}
		}
		
		if(interrupted){
			selfInterrupt();
		}
	}
	
	/**
	 * try to acqure the lock within zk, including setting the node created , and node we are waiting for in current local structure
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private boolean tryAcquire() throws KeeperException, InterruptedException{
		String data = String.valueOf(System.nanoTime());
		
		String createdPath = nodeCreated;
		if(createdPath == null || createdPath.length() == 0){
			nodeCreated = createdPath = zk.create(nodeNamePrefix, data.getBytes() , 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			//System.out.println("NodeCreated : " + nodeCreated);
		}//else create before
		
		int mySequence = getPathSeq(createdPath,nodeNamePrefix);
		int numLen = createdPath.length() - nodeNamePrefix.length();
		int maxSeqButLessThanMe = Integer.MIN_VALUE;
		List<String> children = zk.getChildren(znode, true);//true is important 
		//we also must observe this event.in case when we get the stale children list  
		for(String child : children){
			int sequence = getPathSeq(child,nodeName);
			if(sequence > maxSeqButLessThanMe && sequence < mySequence){
				maxSeqButLessThanMe = sequence;
			}
		}
		if(maxSeqButLessThanMe == Integer.MIN_VALUE){
			logger.debug("get lock");
			return true;
		}else{
			nodeWaitingFor = nodeNamePrefix + String.format("%0"+numLen+"d", maxSeqButLessThanMe);
			//logger.debug("waiting for " + nodeWaitingFor);
			return false;
		}
	}
	
	private void selfInterrupt(){
		Thread.currentThread().interrupt();
	}
	
	@Override
	public void lockInterruptibly() throws InterruptedException {
		checkIfDestroyAlready();
		
		synchronized(this){
			while(!destroy){
				try {
					if(!tryAcquire()){
						zk.exists(nodeWaitingFor, true);
						wait();
					}
				} catch (InterruptedException e) {
					throw e;
				} catch (KeeperException e) {
					throw new DistributedLockObtainException(e);
				}
			}
		}
	}

	@Override
	public boolean tryLock() {
		checkIfDestroyAlready();
		
		try { 
		    return tryAcquire();
		} catch (KeeperException | InterruptedException e) {
			return false;
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		checkIfDestroyAlready();
		
		final long start = System.nanoTime();
		final long timeout = TimeUnit.NANOSECONDS.convert(time, unit);
		synchronized(this){
			while(!destroy){
				long left = timeout - (System.nanoTime() - start);
				if(left <= 0)
					break;
				try {
					if(tryAcquire()){
						return true;
					} else{
						wait(TimeUnit.MILLISECONDS.convert(left, TimeUnit.NANOSECONDS));
					}
				} catch (KeeperException e) { //session timeout ..... TODo:set state
					throw new DistributedLockObtainException(e); 
					//what shold i do when a unexpectable KeeperException occured
				}
			}
		}
		return false;
	}

	public void unlock(){
		logger.debug("unlock");
		if(nodeCreated == null || nodeCreated.length() == 0)
			return;
		zk.delete(nodeCreated, -1, this, null);
		nodeCreated = "";
	}
	
	@Override
	public void processResult(int rc, String path, Object ctx) {
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}

	public boolean destroy(){
		doNotify();
		
		destroy = true;
		
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zooKeeper = ZookeeperClientFactory.newZooKeeper(
				ZookeeperCfgConstants.CONNECTSTRING,
				ZookeeperCfgConstants.SESSION_TIMEOUT,
				Watchers.DUMMY);
		
//		List<String> children = zooKeeper.getChildren(defaultZnode, false);
//		for(String child : children){
//			System.out.println(child.substring("dfl".length()+1));
//		}
		
		
		String path = zooKeeper.create(defaultZnode + "/dfl", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(path);
		
		zooKeeper.close();
		
//		System.out.println(String.format("%010d", 1));
	}
}
