package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.ex.DistributedBarrierException;
import com.kibou.zk.util.ZookeeperClientFactory;

/**
 * @see CyclicBarrier
 */
public class DistributedBarrier implements Watcher, StatCallback{
	
	private final String znode;
	private final ZooKeeper zk;
	
	private final int parties;
	
	private final String nodeNamePrefix = "barrier";
	private final String nodeName;
	
	private String nodeCreated ="";
	private volatile String nodeWaitingFor = "";//当前节点的后一个节点
	
	private boolean broken = false;
	
	public DistributedBarrier(String znode,int parties){
		assert znode != null;
		this.znode = znode;
		this.nodeName = znode + "/" + nodeNamePrefix;
		this.parties = parties;
		try {
			zk = ZookeeperClientFactory.newZooKeeper(
					ZookeeperCfgConstants.CONNECTSTRING,
					ZookeeperCfgConstants.SESSION_TIMEOUT,
					this);
		} catch (IOException  e) {
			throw new DistributedBarrierException(e);
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		EventType type = event.getType();
		String path = event.getPath();
		if(path != null){
			if(path.equals(znode)){
				switch(type){
					case NodeChildrenChanged:
						doNotify(); break;
					default:
						try {
							zk.getChildren(znode, true);
						} catch (KeeperException | InterruptedException e) {
							doNotify();
							e.printStackTrace();
						}
						break;
				}
			}else if(path.equals(nodeWaitingFor)){
				switch(type){
					case NodeDeleted:
						doNotify(); break;
					default:
					try {
						zk.exists(nodeWaitingFor, true);
					} catch (KeeperException | InterruptedException e) {
						doNotify();
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	private void doNotify(){
		synchronized(this){
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
	
	public int await(long timeount,TimeUnit unit) throws InterruptedException, BrokenBarrierException{
		return -1;
	}
	
	/*private void breakBarrier(){
		broken = true;
	}*/
	
	public int await() throws BrokenBarrierException{
		while(!broken){
			synchronized (this) {
				try {
					String createdPath = nodeCreated;
					if(createdPath == null || createdPath.length() == 0 ){
						String data = String.valueOf(System.nanoTime());
						createdPath = zk.create(nodeName, data.getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.EPHEMERAL_SEQUENTIAL);
					}
	
					int mySequence = getPathSeq(createdPath,nodeNamePrefix);
					int numLen = createdPath.length() - nodeNamePrefix.length();
					int count = 0;
					List<String> children = zk.getChildren(znode, true);
					for(String child : children){
						int sequence = getPathSeq(child,nodeName);
						if(mySequence > sequence) count++;
					}
					
					nodeWaitingFor = nodeNamePrefix + String.format("%0"+numLen+"d", mySequence+1);
					zk.exists(nodeWaitingFor, true, this, null);
					
					if(children.size() == parties){
						return count;
					}else{
						wait();
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
					broken = true;
				}
			}
		}
		throw new BrokenBarrierException();//FIXME use new Distribute Zk Exception
	}
	
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		
	}
}
