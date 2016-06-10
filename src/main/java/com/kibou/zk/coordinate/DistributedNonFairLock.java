package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
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
//	private CountDownLatch connectionDoneCdl = new CountDownLatch(1);
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
//			connectionDoneCdl.await();
			zk.exists(znode, true);
		} catch (IOException | KeeperException | InterruptedException  e) {
			throw new DistributedLockObtainException(e);
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(!destroy){
			EventType type = event.getType();
			if(znode.equals(event.getPath())){
				switch (type) {
					case NodeDeleted:
						doNotify();	
						break;
					default:
						break;
				}
				//re-register this,cause notify does not mean you can 100% get the lock; 
				try {
					zk.exists(znode, true);
					/*zk.exists(znode, true, new StatCallback(){}, null);*/
				} catch (KeeperException | InterruptedException e) { //ConnectionLossException
					if(e instanceof ConnectionLossException){//server clapses?
						if(destroy){} //can suppress, 锁线程关闭了连接过程中, zk线程仍可能收到事件响应,但这时这情况可以忽略
						//wait for reconnect ? or detroy the lock!
					}//state = .. 
					doNotify();
					e.printStackTrace();
				}
			}else{
				/*if(event.getState() == KeeperState.SyncConnected){
					connectionDoneCdl.countDown();
				}*/
				logger.debug(event.toString());
			}
		}
	}
	
	private void doNotify(){
		synchronized (this) {
			notify();
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
	
	private boolean tryAcquire() {//doCreateLockNode
		try{
		//TODo : in product env we should use IP/Hostname instead of the time/epoch
			byte[] addr = Thread.currentThread().getName().getBytes();//byte[] addr = InetAddress.getLocalHost().getAddress();
			
			/*byte[] lockHolder = zk.getData(znode, true, null);
			if(lockHolder != null){
				return Arrays.equals(addr, lockHolder);
			}*/
			
			zk.create(znode, addr, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			logger.debug("get lock");
			return true;
		}catch(NodeExistsException | InterruptedException e){
			return false;
		}catch(KeeperException e){
			throw new DistributedLockObtainException(e);
		}
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
		
		while(!destroy){
			synchronized(this){
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
		synchronized(this){
			while(!destroy){
				long left = timeout - (System.nanoTime() - start);
				if(left <= 0)
					break;
				
				if(tryAcquire())
					return true;
				else{
					wait(TimeUnit.MILLISECONDS.convert(left, TimeUnit.NANOSECONDS));
				}
			}
		}
		return false;
	}
	
	public void lock() throws DistributedLockObtainException { //uninterruptable
		checkIfDestroyAlready();
		
		boolean isInterrupted = false;
		while(!destroy){
			synchronized(this){
				if(tryAcquire())
					return;
				else{
					try{
						wait();
					} catch (InterruptedException e1) {
						isInterrupted = true;
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
