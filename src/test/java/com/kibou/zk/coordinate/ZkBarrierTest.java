package com.kibou.zk.coordinate;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

import com.kibou.zk.ex.BrokenDistributedBarrierException;
import com.kibou.zk.util.Watchers;
import com.kibou.zk.util.ZookeeperClientFactory;

public class ZkBarrierTest{
	
	String zpath = "/test";
	
	private interface ThreadFactory {
		public Thread newThread(String znode,int parties);
	}
	
	private boolean awaitAll(Thread[] threads){
		for (int i = 0; i < threads.length; ++i) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return true;
	}
	
	private Thread[] newPartyThreads(String zpath,int parties,ThreadFactory threadFactory){
		Thread[] threads = new Thread[parties];
		for(int i = 0; i < parties; ++i){
			threads[i] = threadFactory.newThread(zpath, parties);
		}
		return threads;
	}
	
	private void testBarrierWithOneSlowThread(String zpath,int parties,ThreadFactory threadFactory)
			throws InterruptedException{
		
		Thread[] threads = newPartyThreads(zpath, parties, threadFactory);
		//1.
		for(int i = 0; i < parties; ++i){
			if(i != parties - 1 )  threads[i].start();
		}
		//2.
		if(parties != 1){
			Thread.sleep(2000);
			threads[parties - 1].start();
		}
		
		awaitAll(threads);
		System.out.println(ai.get());
	}
	
	@Test
	public void test111() throws Exception{
		ZooKeeper zooKeeper = ZookeeperClientFactory.newZooKeeper("127.0.0.1:2181", 3000,Watchers.DUMMY);	
		zooKeeper.create("/test/barrier",ZookeeperHelper.emptyData(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		zooKeeper.close();
	}
	
	@Test
	public void testBarrier() throws Throwable {
		ZooKeeper zooKeeper = ZookeeperClientFactory.newZooKeeper("127.0.0.1:2181", 3000,Watchers.DUMMY);
		zooKeeper.setData(zpath, new byte[0], -1);
		zooKeeper.close();
		
		testBarrierWithOneSlowThread(zpath, 5, new ThreadFactory() {
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(new DistributedOneGenerationBarrier(znode,parties));
			}
		});
	}
	@Test
	public void testBarrierv2() throws Throwable {
		testBarrierWithOneSlowThread(zpath, 5, new ThreadFactory() {
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(new DistributedOneGenerationBarrierV2(znode,parties));
			}
		});
	}
	
	
	private AtomicInteger ai = new AtomicInteger(0);
	
	class ZKbWorker extends Thread{
		
		private DistributedBarrier barrier;
		
		public ZKbWorker(DistributedBarrier barrier){
			this.barrier = barrier;
		}
		@Override
		public void run() {
			try {
				barrier.await();
				doWork();
			} catch (BrokenDistributedBarrierException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}catch (Throwable e) {
				e.printStackTrace();
			}finally{
				/*try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/
				barrier.destroy();
			}
		}
		private void doWork() {
			System.out.println(Thread.currentThread());
			ai.incrementAndGet();
		}
	}
}
