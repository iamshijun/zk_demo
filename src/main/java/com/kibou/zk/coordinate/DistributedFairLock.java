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
import com.kibou.zk.util.ZookeeperClientFactory;

//make this Reentrant able
public class DistributedFairLock implements Watcher, VoidCallback, DistributedLock { // lock without herd effect (WEF)
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static String defaultZnode = "/lock/fairLock"; //default
	
	private final ZooKeeper zk;
	private final String zparentNode;
	
	private final String nodeName = "dfl";
	private String zpath;

	private boolean destroy = false;
	
	private String nodeCreated = ""; // currentNode , use by lock,unlock cannot seen by zk thread
	
	public DistributedFairLock() {
		this(defaultZnode, ZookeeperCfgConstants.CONNECTSTRING, ZookeeperCfgConstants.SESSION_TIMEOUT);
	}
	
	public DistributedFairLock(String zparentPath, String connectString, int sessionTimeout) {
		assert zparentPath != null;
		this.zparentNode = zparentPath;
		this.zpath = zparentPath + "/" + nodeName;
		try {
			zk = ZookeeperClientFactory.newZooKeeper(connectString, sessionTimeout, this);
		} catch (IOException  e) {
			throw new DistributedLockObtainException(e);
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if(path != null && path.equals(zparentNode)){
			EventType eventType = event.getType();
			switch (eventType){
				case NodeChildrenChanged:
					doNotify(); break;
				default:
					try {
						zk.exists(zparentNode, true);
						logger.debug("Got event : " + event.getType());
					} catch (KeeperException | InterruptedException e) {
						doNotify(); 	
						e.printStackTrace();
					}
					break;
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
		try{
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
		
		while(!destroy){
			synchronized(this){
				if(tryAcquire())
					return;
				else{
					try {
						wait();
					} catch (InterruptedException e) {
						interrupted = true;
					}
				}
			}
		}
		
		if(interrupted){
			selfInterrupt();
		}
	}
	
	/*private boolean doWait(){
		synchronized (this) {
			try {
				wait();
			} catch (InterruptedException e) {
				return true;
			}
		}
		return false;
	}*/
	
	/**
	 * try to acqure the lock within zk, including setting the node created , and node we are waiting for in current local structure
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private boolean tryAcquire() throws DistributedLockObtainException {
		try{
		String createdPath = nodeCreated;
		if(createdPath == null || createdPath.length() == 0){
			String data = String.valueOf(System.nanoTime());
			//byte[] addr = InetAddress.getLocalHost().getAddress(); 
			createdPath = nodeCreated = zk.create(zpath, data.getBytes() , 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		}//else create before
		
		int mySequence = getPathSeq(createdPath,zpath);
		int maxSeqButLessThanMe = Integer.MIN_VALUE;
		List<String> children = zk.getChildren(zparentNode, true);//true is important 
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
			return false;
		}
		
		}catch(InterruptedException e){
			return false;
		}catch(KeeperException e){
			throw new DistributedLockObtainException(e);
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
				if(tryAcquire()){
					return;
				}else{
					wait();
				}
			}
		}
	}

	@Override
	public boolean tryLock() {
		checkIfDestroyAlready();
		
		try { 
		    return tryAcquire();
		} catch (DistributedLockObtainException e) {
			return false;
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		checkIfDestroyAlready();
		
		final long start = System.nanoTime();
		final long timeout = TimeUnit.NANOSECONDS.convert(time, unit);
		
		while(!destroy){
			synchronized(this){
				long left = timeout - (System.nanoTime() - start);
				if(left <= 0)
					break;
				if(tryAcquire()){
					return true;
				} else{
					wait(TimeUnit.MILLISECONDS.convert(left, TimeUnit.NANOSECONDS));
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
	
}
