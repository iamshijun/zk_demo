package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
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

/**
 * maybe you can change this with AQS( @see AbstractQueuedSynchronizer)
 */
//TODO make this Reentrant able,(per Process or Thread ?)
public class DistributedNonFairLock implements Watcher, VoidCallback, DistributedLock{
	private Logger logger = LoggerFactory.getLogger(getClass());

	private static String defaultZnode = "/lock/nonfairLock"; //default
	
	private final ZooKeeper zk;
	private final String znode;

	private boolean destroy = false;
	
	//all states
	final int destroyed = -1; 
	final int normal = 0;
	final int unusable = 1;//just like pause waiting for resume( SyncConnect? reconnect) 
	
//	private int state = 0;
	
	public DistributedNonFairLock() {
		this(defaultZnode);
	}
	
	public DistributedNonFairLock(String znode) {
		assert znode != null;
		this.znode = znode;
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
		EventType type = event.getType();
		if(znode.equals(event.getPath())){
			switch (type) {
				case NodeDeleted:
					doNotify();	break;
				default:		break;
			}
		}else{
			logger.debug(event.toString());
		}
		//re-register this
		if(!destroy){
			try {
				zk.exists(znode, true);
			} catch (KeeperException | InterruptedException e) { //loss conection....
				if(e instanceof KeeperException){
					//state = 
				}
				doNotify();
				e.printStackTrace();
			}
		}
	}
	
	private void doNotify(){
		synchronized (this) {
			notify();
		}
	}
	
	private boolean tryAcquire() throws KeeperException, InterruptedException {//doCreateLockNode
		//TODo : in product env we should use IP/Hostname instead of the time/epoch
		String data = String.valueOf(System.nanoTime());
		zk.create(znode, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		logger.debug("get lock");
		return true;
	}
	
	private void checkIfDestroyAlready() {
		if(destroy){
			throw new DistributedLockDestroyedException("Lock has been destroyed");
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
					tryAcquire();
					return;
				} catch (KeeperException e) {
					if(e instanceof NodeExistsException){
						wait();
					}else{ //session timeout .....
						throw new DistributedLockObtainException(e);
					}
				}
			}
		}
		
	}
	
	@Override
	public boolean tryLock() {
		checkIfDestroyAlready();
		
		while(!destroy){
			try { 
				tryAcquire();
				return true;
			} catch (KeeperException | InterruptedException e) {
				if(e instanceof NodeExistsException){
					return false;
				}
			}
		}
		return false;
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
					return tryAcquire();
				} catch (KeeperException e) {
					if(e instanceof NodeExistsException){//Ops...,bad luck,wait again please
						wait(TimeUnit.MILLISECONDS.convert(left, TimeUnit.NANOSECONDS));
					}
					else{ //session timeout ..... TODo:set state
						throw new DistributedLockObtainException(e); 
						//what shold i do when a unexpectable KeeperException occured
					}
				}
			}
		}
		return false;
	}
	
	public void lock(){ //uninterruptable
		checkIfDestroyAlready();
		
		boolean isInterrupted = false;
		synchronized(this){
			while(!destroy){
				try {
					tryAcquire();
					return;
				} catch (InterruptedException e) {
					isInterrupted = true;
				} catch (KeeperException e) {
					if(e instanceof NodeExistsException){
						try {
							wait();
						} catch (InterruptedException e1) {
							isInterrupted = true;
						}
					}
					else{ //session timeout .....
						throw new DistributedLockObtainException(e);
					}
				}
			}
		}
		
		if(isInterrupted){
			selfInterrupt();
		}
	}
	
	public void unlock(){
		logger.debug("unlock");
		zk.delete(znode, -1, this, null);
	}

	@Override
	public void processResult(int rc, String path, Object ctx) {
		/*switch(Code.get(rc)){
			case :
				break;
			default:
				break;
		}*/
		//System.out.println(Code.get(rc));
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
	
	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}
}
