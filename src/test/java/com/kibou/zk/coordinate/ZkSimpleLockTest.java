package com.kibou.zk.coordinate;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.kibou.zk.ex.DistributedLockObtainException;

public class ZkSimpleLockTest {
	
	private interface ThreadFactory{
		/**
		 * @param cb
		 * 		make sure they work just parallel-like;
		 * @param workLoop
		 * 		after work 'workLoop' times , return
		 * @param args
		 * @return
		 */
		public Thread newThread(CyclicBarrier cb,int workLoop,Object... args);
	}
	
	private interface TestCallback{
		public void beforeStart();
		public void afterCompletion();
		
		public TestCallback DEFAULT = new TestCallback() {
			public void beforeStart() {}
			public void afterCompletion() {}
		};
	}
	private TestCallback defaultTestCallback = new TestCallback() {
		public void beforeStart() {}
		public void afterCompletion() {
			System.out.println("[" + testName.getMethodName() + "] competition = " + safeCompetition.get());
		}
	};
	
	@Rule
	public TestName testName = new TestName();
	
	@Before
	public void setUP(){
		safeCompetition.set(0);
	}
	
	public void testTemplate(ThreadFactory threadFactory,int workLoop,Object... args){
		testTemplate(threadFactory, workLoop, defaultTestCallback, args);
	}
	
	public void testTemplate(ThreadFactory threadFactory,int workLoop,TestCallback testCallback,Object... args){
		if(testCallback == null){
			testCallback = TestCallback.DEFAULT;
		}
		
		int threadCount = 5;
		CyclicBarrier cb = new CyclicBarrier(threadCount + 1);
		
		Thread[] threads = new Thread[threadCount];
		for(int i = 0; i < threadCount; ++i){
			threads[i] = threadFactory.newThread(cb,workLoop,args);
			threads[i].start();
		}
		
		try {
			cb.await();
			
			for (int i = 0; i < threadCount; ++i) {
				threads[i].join();
			}
			
			testCallback.afterCompletion();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testWithoutZK(){
		testTemplate(new ThreadFactory() {
			@Override
			public Thread newThread(CyclicBarrier cb, int workLoop, Object... args) {
				return new Worker(cb, workLoop);
			}
		},100);
	}
	
	@Test
	public void testZkCliWorkerWithNonfaiLock(){
		testTemplate(new ThreadFactory() {
			@Override
			public Thread newThread(CyclicBarrier cb, int workLoop, Object... args) {
				return new ZkCliWorker(cb, workLoop/*,new DistributedNonFairLock()*/);
			}
		},10);
	}
	
	@Test
	public void testZkCliSmartWorkerWithNonfaiLock(){
		testTemplate(new ThreadFactory() {
			@Override
			public Thread newThread(CyclicBarrier cb, int workLoop, Object... args) {
				return new ZkCliSmartWorker(cb, workLoop/*,new DistributedNonFairLock()*/);
			}
		},5);
	}
	
	@Test
	public void testZkCliWorkerWithFaiLock(){
		testTemplate(new ThreadFactory() {
			@Override
			public Thread newThread(CyclicBarrier cb, int workLoop, Object... args) {
				return new ZkCliWorker(cb, workLoop, new DistributedFairLock());
			}
		},5);
	}
	
	@Test
	public void testZkCliSmartWorkerWithFaiLock(){
		testTemplate(new ThreadFactory() {
			@Override
			public Thread newThread(CyclicBarrier cb, int workLoop, Object... args) {
				return new ZkCliSmartWorker(cb, workLoop, new DistributedFairLock());
			}
		},5);
	}
	

	private AtomicInteger safeCompetition = new AtomicInteger(0);
//	private int competition = 0;
	
	class Worker extends Thread {
		//ps : 其实是competition不是threadsafe,请注意threadsafe最佳实践是封装! (将java并发编程实战)
		
		final int retriveCnt;
		final CyclicBarrier cycliBarrier;
		
		public Worker(CyclicBarrier cycliBarrier,int retriveCnt) {
			this.cycliBarrier = cycliBarrier;
			this.retriveCnt = retriveCnt;
		}
		
		@Override
		public void run() {
			try {
				cycliBarrier.await();
				doWork();
			} catch (BrokenBarrierException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		protected void doWork() throws InterruptedException{
			for(int i = 0; i < retriveCnt; ++i){
				//competition++;
				safeCompetition.incrementAndGet();
			}
		}
	}
	
	class ZkCliWorker extends Worker {

		protected DistributedLock disSimpleLock;
		
		public ZkCliWorker(CyclicBarrier cycliBarrier,int retriveCnt){
			this(cycliBarrier,retriveCnt,new DistributedNonFairLock());
		}
		
		public ZkCliWorker(CyclicBarrier cycliBarrier,int retriveCnt,DistributedLock lock){
			super(cycliBarrier,retriveCnt);
			this.disSimpleLock = lock;
		}
		
		protected void doWork() throws InterruptedException{
			ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
			for(int i = 0; i < retriveCnt; ++i){
				try {
					//Thread.sleep(500);
					Thread.sleep(threadLocalRandom.nextInt(200, 1000));
					disSimpleLock.lock();
					safeCompetition.incrementAndGet();
				}catch(DistributedLockObtainException e){
					e.printStackTrace();//continue;
					i--;
				}finally{
					disSimpleLock.unlock();
				}
			}
			
			disSimpleLock.destroy();
		}
	}
	
	class ZkCliSmartWorker extends ZkCliWorker{

		public ZkCliSmartWorker(CyclicBarrier cycliBarrier, int retriveCnt) {
			this(cycliBarrier, retriveCnt, new DistributedNonFairLock());
		}
		public ZkCliSmartWorker(CyclicBarrier cycliBarrier,int retriveCnt,DistributedLock lock){
			super(cycliBarrier, retriveCnt, lock);
		}
		
		@Override
		protected void doWork() throws  InterruptedException {
			ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
			for(int i = 0; i < retriveCnt; ++i){
				Thread.sleep(500);//不给缓冲的话 每次都是自己拿到锁, 让其他节点获取到锁
				while(!disSimpleLock.tryLock(1, TimeUnit.SECONDS)){
					//do another thing!
				}
				try {
					Thread.sleep(threadLocalRandom.nextInt(1000, 3000));
					safeCompetition.incrementAndGet();
				} finally{
					disSimpleLock.unlock();
				}
			}
			
			disSimpleLock.destroy();
		}
	}
}
