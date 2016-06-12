package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.ex.BrokenDistributedBarrierException;
import com.kibou.zk.ex.DistributedBarrierException;
import com.kibou.zk.util.ZookeeperClientFactory;

/**
 * 需要总协调者将传入的指定path创建好,或者说是reset (data清空)/或者是整个path删除后创建
 * 需要保证里面没有任何的child和data不为"start"最好是空值,
 */
public class DistributedOneGenerationBarrier implements Watcher, StatCallback, DistributedBarrier{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private final String zparentPath;
	private final ZooKeeper zk;
	
	private final int parties;
	
	private final String nodeName = "barrier";
	private final String zpath;
	
	private final byte[] startbytes = "start".getBytes(); 
	private final byte[] destroybytes = "destroy".getBytes(); 
	
	private String nodeCreated ="";
	
	private volatile String prevNodeWaitingFor = "";
	private volatile String nextNodeWaitingFor = "";
	
	/*private Generation generation;
	private static class Generation {
	}*/
	
	private volatile int state = 0;
	private final static int allReach = 1;
	private final static int broken = -1;
	private final static int destroy = -2;
	
	
	private void breakBarrier(){ state = broken; 		}
	private void setAllReach()    { state = allReach;	}
	private void setDestroy()    { state = destroy;	}
	
	private boolean isAllReach(){ return state == allReach;	}
	private boolean isBroken() {return state == broken; }
	private boolean isDestroy(){ return state == destroy;}
	
	//private boolean isUsable(){ return state != broken && state != destroy;};
	
//	private Thread exclusiveOwnerThread;
	
	/**
	 * @param zparentNode
	 * @param parties
	 */
	public DistributedOneGenerationBarrier(String zparentNode,int parties){
		this(zparentNode, parties, ZookeeperCfgConstants.CONNECTSTRING, ZookeeperCfgConstants.SESSION_TIMEOUT);
	}
	public DistributedOneGenerationBarrier(String zparentNode,int parties,String connectString, int sessionTimeout){
		assert zparentNode != null;
		this.zparentPath = zparentNode;
		this.zpath = zparentNode + "/" + nodeName;
		this.parties = parties;
		try {
			zk = ZookeeperClientFactory.newZooKeeper(connectString, sessionTimeout, this);
		} catch (IOException  e) {
			throw new DistributedBarrierException(e);
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(isDestroy())
			return;
		
		logger.trace(event.toString());
		try{
			if(zparentPath.equals(event.getPath())){
				switch(event.getType()){
					case NodeDeleted:
						breakBarrier();
						doNotify(); 
						break;
					default:
						byte[] data = zk.getData(zparentPath, true, null);
						if(data != null && Arrays.equals(startbytes, data)){
							setAllReach();
							doNotify();
						}
						break;
				}
			}else if(nextNodeWaitingFor.equals(event.getPath())
					|| prevNodeWaitingFor.equals(event.getPath())){
				Stat stat = zk.exists(event.getPath(), true);
				if(stat == null){
					breakBarrier();
					doNotify();
					
					zk.setData(zpath, destroybytes, -1, this, null);
				}
			}else{
				//path == null && SyncConnect event
				//logger.debug(event.toString());
			}
		} catch (KeeperException | InterruptedException e) {
			if(ConnectionLossException.class.isInstance(e)){
				breakBarrier();
			}
			doNotify();
			e.printStackTrace();
		}
	}
	
	private void doNotify(){
		synchronized(this){
			notify();
		}
	}
	
	private List<Integer> getNodeSequences(List<String> children){
		//System.out.println(children);
		List<Integer> sequences = 
				ZookeeperHelper.getNodeSequences(children,nodeName);
		Collections.sort(sequences);
		return sequences;
	}
	
	boolean checkBarrierIsBroken(List<Integer> sequences){//make sure it's sorted before
		for(int i = 0 ; i < sequences.size() - 1;++i){
			if(sequences.get(i) + 1 != sequences.get(i+1)){
				breakBarrier();
				return true;
			}
		}
		return false;
	}
	
	public int await(long time,TimeUnit unit) 
			throws InterruptedException, BrokenDistributedBarrierException, TimeoutException, DistributedBarrierException{
		
		if(!isBroken()){
			int myIndex = init();
			
			final long start = System.nanoTime();
			final long timeout = TimeUnit.NANOSECONDS.convert(time, unit);
			
			for(;;){
				long left = timeout - (System.nanoTime() - start);
				if(left <= 0 ){
					zk.setData(zpath, destroybytes, -1, this, null);//best effor
					throw new TimeoutException();
				}
				synchronized (this) {
					if(isAllReach())
						return myIndex;
					if(isBroken())
						break;
					logger.trace("wait");
					
					wait(TimeUnit.MILLISECONDS.convert(left, TimeUnit.NANOSECONDS));
				}
			}
		}
		
		throw new BrokenDistributedBarrierException();
	}
	
	public int await()
			throws InterruptedException, BrokenDistributedBarrierException, DistributedBarrierException{
		if(!isBroken()){
			int myIndex = init();
			for(;;){
				synchronized (this) {
					if(isAllReach())
						return myIndex;
					if(isBroken())
						break;
					logger.trace("wait");
					this.wait();
				}
			}
		}
		
		throw new BrokenDistributedBarrierException();
	}
	private int init() throws BrokenDistributedBarrierException {
		try{
			int myIndex = -1;
			//1. create -s -e node 
			nodeCreated = zk.create(zpath, String.valueOf(System.nanoTime()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			
			//2. check where am i
			int mySequence = ZookeeperHelper.getPathSeq(nodeCreated, zpath);
			List<Integer> nodeSequences = getNodeSequences(zk.getChildren(zparentPath, false));
			for(int sequence : nodeSequences){
				if(mySequence > sequence) myIndex++;
			}
			
			//3. setup flag /checkout flag
			if(myIndex == parties - 1){
				logger.trace("i am the last one");
				zk.setData(zparentPath, startbytes, -1);//set 'start' flag
				return myIndex;
			}
			//4. check if started or destroyed 
			byte[] data = zk.getData(zparentPath, true, null);
			//logger.trace(new String(data));
			if(data != null){
				if(Arrays.equals(startbytes, data))
					return myIndex;
				if(Arrays.equals(destroybytes, data)){
					breakBarrier();
					throw new BrokenDistributedBarrierException();
				}
			}
			
			//5. waiting for prev & next node
			nextNodeWaitingFor = zpath + String.format("%0" + (nodeCreated.length() - zpath.length()) + "d", mySequence + 1);
			zk.exists(nextNodeWaitingFor, true, this, null);
			if(myIndex != 0){
				prevNodeWaitingFor = zpath + String.format("%0" + (nodeCreated.length() - zpath.length()) + "d", mySequence -1);
				zk.exists(prevNodeWaitingFor, true, this, null);
			}
			return myIndex;
		} catch (KeeperException | InterruptedException e) {
			leave();
			throw new DistributedBarrierException(e);
		}
	}
	
	public void destroy(){
		logger.debug("destroy");
		
		setDestroy();
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void leave(){
		logger.debug("leave");
		
		if(nodeCreated == null || nodeCreated.length() == 0)
			return;
		
		breakBarrier();
		try {
			zk.delete(nodeCreated, -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		switch(Code.get(rc)){
			case SESSIONEXPIRED:
			case CONNECTIONLOSS:
				logger.debug("processResult rc : {}" ,rc);
				break;
			default : 
				break;
		}
	}
}
