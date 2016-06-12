package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kibou.zk.util.Watchers;
import com.kibou.zk.util.ZookeeperClientFactory;

public class ZkBarrierTest{
	
	private static final String ZPATH = "/test";
	
	private static Logger logger  = LoggerFactory.getLogger(ZkBarrierTest.class);
	
	private interface ThreadFactory {
		public Thread newThread(String znode,int parties);
	}
	
	private boolean awaitAll(long timeout,TimeUnit unit,Thread... threads){
		if(threads == null || threads.length == 0)
			return true;
		for (int i = 0; i < threads.length; ++i) {
			try {
				if(timeout ==0)
					threads[i].join();
				else
					threads[i].join(TimeUnit.MILLISECONDS.convert(timeout, unit));
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return true;
	}
	
	private boolean awaitAll(Thread... threads){
		return awaitAll(0,null,threads);
	}
	
	private Thread[] newPartyThreads(String zpath,int parties,ThreadFactory threadFactory){
		Thread[] threads = new Thread[parties];
		for(int i = 0; i < parties; ++i){
			threads[i] = threadFactory.newThread(zpath, parties);
		}
		return threads;
	}
	
	
	static ZooKeeper zkInitializer;
	
	@BeforeClass
	public static void init() throws IOException{
		zkInitializer = ZookeeperClientFactory.newZooKeeper("127.0.0.1:2181", 3000,Watchers.DUMMY);
	}
	
	@AfterClass
	public static void destroy() throws InterruptedException{
		zkInitializer.close();
	}
	
	private void doTestBarrierWithOneSlowThread(String zpath,int parties,ThreadFactory threadFactory) throws InterruptedException{
		doTestBarrierWithOneSlowThread(zpath, parties, threadFactory, 2, TimeUnit.SECONDS);
	}
	private void doTestBarrierWithOneSlowThread(String zpath,int parties,ThreadFactory threadFactory,long timeout,TimeUnit unit)
			throws InterruptedException{
		
		Thread[] threads = newPartyThreads(zpath, parties, threadFactory);
		//1.
		for(int i = 0; i < parties; ++i){
			if(i != parties - 1 )  threads[i].start();
		}
		//2.
		if(parties != 1){
			unit.sleep(timeout);
			threads[parties - 1].start();
		}
		
		awaitAll(threads);
		System.out.println(ai.get());
	}
	
	private void doTestBarrierBroken(String zpath,int parties,ThreadFactory threadFactory,TestCallback testCallback)
			throws InterruptedException{
		if(parties <= 3){
			throw new IllegalArgumentException("parties should larger then 3");
		}
		if(testCallback == null)
			testCallback = TestCallback.DEFAULT;
		//1
		testCallback.beforeStart();
			
		Thread[] threads = newPartyThreads(zpath, parties, threadFactory);
		for(int i = 0 ; i < 3;++i){ //0,1,2 start
			threads[i].start();
		}
		//2
		testCallback.beforeCompletion();
		
		awaitAll(threads[0],threads[1],threads[2]);
		//3
		testCallback.afterCompletion();
	}
	
	@Test
	public void testDepBarrier() throws Throwable {
		zkInitializer.setData(ZPATH, new byte[0], -1);
		
		doTestBarrierWithOneSlowThread(ZPATH, 5, new ThreadFactory() {
			@SuppressWarnings("deprecation")
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(new DistributedOneGenerationBarrierV1(znode,parties));
			}
		});
	}
	
	
	
	@Test
	public void testBarrier() throws Throwable {
		zkInitializer.setData(ZPATH, new byte[0], -1);
		
		doTestBarrierWithOneSlowThread(ZPATH, 5, new ThreadFactory() {
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(new DistributedOneGenerationBarrier(znode,parties));
			}
		});
	}
	
	@Test
	public void testBarrierBroken() throws Throwable {
		zkInitializer.setData(ZPATH, new byte[0], -1);
		
		final Thread rmrThread = new Thread(){
			public void run() {
				try {
					List<String> children;
					while(true){
						children = zkInitializer.getChildren(ZPATH, false);
						if(children.size() == 0)
							Thread.sleep(1000);
						else
							break;
					}
					System.out.println(children);
					Collections.sort(children);
					
					//String childDelete = children.get(0);
					String childDelete = children.get(children.size() - 1);
					
					System.out.println("ready to delete " + childDelete);
					zkInitializer.delete(ZPATH + "/" + childDelete, -1);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			};
		};
		
		doTestBarrierBroken(ZPATH, 5, new ThreadFactory() {
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(new DistributedOneGenerationBarrier(znode,parties));
			}
		},new TestCallbackAdpater(){
			public void beforeCompletion() {
				rmrThread.start();
			}
			@Override
			public void afterCompletion() {
				System.out.println(ai.get());
			}	
		});
		
		rmrThread.join();
	}
	
	@Test
	public void testBarrierTimeout() throws InterruptedException{
		doTestBarrierWithOneSlowThread(ZPATH, 5, new ThreadFactory() {
			public Thread newThread(String znode, int parties) {
				return new ZKbWorker(
						new DistributedOneGenerationBarrier(znode,parties),
						5,TimeUnit.SECONDS);
			}
		},10,TimeUnit.SECONDS);
	}
	
	
	private AtomicInteger ai = new AtomicInteger(0);
	
	class ZKbWorker extends Thread{
		
		private DistributedBarrier barrier;
		long timeout; TimeUnit unit;
		
		public ZKbWorker(DistributedBarrier barrier,long timeout,TimeUnit unit){
			this.timeout = timeout;
			this.unit = unit;
			this.barrier = barrier;
		}
		public ZKbWorker(DistributedBarrier barrier){
			this(barrier,-1,null);
		}
		public void run() {
			try {
				if(timeout <= 0)
					barrier.await();
				else
					barrier.await(timeout, unit);
				doWork();
			}catch (Throwable e) {
				logger.error(e.getMessage(), e);
			}finally{
				/*try {	Thread.sleep(1000);	} catch (InterruptedException e) {	e.printStackTrace();}*/
				barrier.destroy();
			}
		}
		private void doWork() {
			logger.debug(Thread.currentThread().getName());
			ai.incrementAndGet();
		}
	}
}
