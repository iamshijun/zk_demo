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
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.ex.BrokenDistributedBarrierException;
import com.kibou.zk.ex.DistributedBarrierException;
import com.kibou.zk.util.ZookeeperClientFactory;
import com.kibou.zk.util.ZooKeeperPathHelper;

/**
 * 不能意识到其他节点的断开
 * @deprecated
 */
public class DistributedOneGenerationBarrierV1 implements Watcher, StatCallback, DistributedBarrier{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private final String zparentPath;
	private final ZooKeeper zk;
	
	private final int parties;
	
	private final String nodeName = "barrier";
	private final String zpath;
	private final byte[] startbytes = "start".getBytes(); 
	
	private volatile String nodeCreated ="";
	
	/*private Generation generation;
	private static class Generation {
	}*/
	
	private volatile boolean broken = false;
	private volatile boolean destroy = false;
	
	/**
	 * @param zparentNode
	 * @param parties
	 */
	public DistributedOneGenerationBarrierV1(String zparentNode,int parties){
		this(zparentNode, parties, ZookeeperCfgConstants.CONNECTSTRING, ZookeeperCfgConstants.SESSION_TIMEOUT);
	}
	public DistributedOneGenerationBarrierV1(String zparentNode,int parties,String connectString, int sessionTimeout){
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
		if(destroy)
			return;
		
		EventType type = event.getType();
		String path = event.getPath();
		if(path != null && path.equals(zparentPath)){
			logger.debug(event.toString());
			switch(type){
				case NodeChildrenChanged:
				case NodeDeleted:
				case NodeDataChanged:
					doNotify(); break;
				default:
					try {
						zk.getChildren(zparentPath, true);
					} catch (KeeperException | InterruptedException e) {
						if(ConnectionLossException.class.isInstance(e)){}
						doNotify();
						e.printStackTrace();
					}
					break;
			}
		}else{
			logger.debug(event.toString());
		}
	}
	
	private void doNotify(){
		synchronized(this){
			notify();
		}
	}
	
	private void breakBarrier(){
		broken = true;
	}
	
	private List<Integer> getNodeSequences(List<String> children){
		//System.out.println(children);
		List<Integer> sequences = 
				ZooKeeperPathHelper.nodeSequences(children,nodeName);
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
	
	private int myIndex = -1 ;
	
	public int await(long timeout,TimeUnit unit) 
			throws InterruptedException, BrokenDistributedBarrierException,TimeoutException, DistributedBarrierException{
		return -1;
	}
	
	public int await()
			throws InterruptedException, BrokenDistributedBarrierException, DistributedBarrierException{
		if(!broken){
			
			String createdPath = nodeCreated;
			if(createdPath == null || createdPath.length() == 0 ){
				String data = String.valueOf(System.nanoTime());
				try {
					createdPath = nodeCreated = zk.create(zpath, data.getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.EPHEMERAL_SEQUENTIAL);
				} catch (KeeperException | InterruptedException e) {
					throw new DistributedBarrierException(e);
				}
			}
			
			for(;!broken;){
				synchronized (this) {
					try {
	
						//int numLen = createdPath.length() - zpath.length();
						if(myIndex == -1){
							myIndex = 0;
							//check where am i
							List<Integer> nodeSequences = getNodeSequences(zk.getChildren(zparentPath, true));
							int mySequence = ZooKeeperPathHelper.seqOfPath(createdPath,zpath);
							for(int sequence : nodeSequences){
								if(mySequence > sequence) myIndex++;
							}
							if(myIndex == parties - 1){
								zk.setData(zparentPath, startbytes, -1);
								logger.trace("i am the last one");
								return myIndex;
							}
						}else{
							byte[] data = zk.getData(zparentPath, true, null);
							logger.debug(new String(data));
							if(data != null && Arrays.equals(startbytes, data)){
								return myIndex;
							}
							
							/* in case some one broken*/
							/*
							List<Integer> nodeSequences = getNodeSequences(zk.getChildren(zparentPath, true));
							System.out.println(nodeSequences);
							if(checkBarrierIsBroken(nodeSequences))
								break;*/
						}
						/*if(myIndex >= parties){return myIndex; //hey guy,you may not belong to this barrier}*/
						
					} catch (KeeperException | InterruptedException e) {
						if(e instanceof NoNodeException){
							//NoNodeException : ParentNode was deleted or ...
						}else{
						}
						e.printStackTrace();
						breakBarrier();
						break;
					}
					logger.trace("wait");
					this.wait();
				}
			}
		}
		
		throw new BrokenDistributedBarrierException();
	}
	
	public void destroy(){
		destroy = true;
		logger.debug("destroy");
		breakBarrier();
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/*private void release(){
		logger.debug("release");
		if(broken)
			return;
		
		breakBarrier();
		try {
			zk.delete(nodeCreated, -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}*/
	
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		switch(Code.get(rc)){
			case SESSIONEXPIRED:
			case CONNECTIONLOSS:
				System.out.println(rc);
				break;
			default : 
				break;
		}
	}
}
