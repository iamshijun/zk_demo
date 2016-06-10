package com.kibou.zk.coordinate;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ZkSimpleLockTest implements Runnable{

	public static void main(String[] args) {
		new ZkSimpleLockTest().run();
	}
	
	public void run(){
		int threadCount = 5;
		CyclicBarrier cb = new CyclicBarrier(threadCount + 1);
		
		Thread[] threads = new Thread[threadCount];
		for(int i = 0; i < threadCount; ++i){
//			threads[i] = new ZkCli(cb,10000);
//			threads[i] = new ZkCliWorker(cb,5);
//			threads[i] = new ZkCliSmartWorker(cb, 5);
			
//			threads[i] = new ZkCliWorker(cb,1,new DistributedFairLock());
			threads[i] = new ZkCliSmartWorker(cb,1,new DistributedFairLock());
			threads[i].start();
		}
		
		try {
			cb.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		try {
			for (int i = 0; i < threadCount; ++i) {
				threads[i].join();
			}
			System.out.println(competition);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private int competition = 0;
	
	class ThreadUnsafeWorker extends Thread {
		//ps : 其实是competition不是threadsafe,请注意threadsafe最佳实践是封装! (将java并发编程实战)
		
		final int retriveCnt;
		final CyclicBarrier cycliBarrier;
		
		public ThreadUnsafeWorker(CyclicBarrier cycliBarrier,int retriveCnt) {
			this.cycliBarrier = cycliBarrier;
			this.retriveCnt = retriveCnt;
		}
		
		@Override
		public void run() {
			try {
				cycliBarrier.await();
			} catch (InterruptedException | BrokenBarrierException e) {
				e.printStackTrace();
				return;
			}
			try {
				doWork();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		protected void doWork() throws InterruptedException{
			for(int i = 0; i < retriveCnt; ++i){
				competition++;
			}
		}
	}
	
	class ZkCliWorker extends ThreadUnsafeWorker {

		protected DistributedLock disSimpleLock;
		
		public ZkCliWorker(CyclicBarrier cycliBarrier,int retriveCnt){
			this(cycliBarrier,retriveCnt,new DistributedNonFairLock());
		}
		
		public ZkCliWorker(CyclicBarrier cycliBarrier,int retriveCnt,DistributedLock lock){
			super(cycliBarrier,retriveCnt);
			this.disSimpleLock = lock;
		}
		
		protected void doWork() throws InterruptedException{
			for(int i = 0; i < retriveCnt; ++i){
				try {
					disSimpleLock.lock();
					//Thread.sleep(1000);
					competition++;
				} finally{
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
					competition++;
				} finally{
					disSimpleLock.unlock();
				}
			}
			
			disSimpleLock.destroy();
		}
	}
}
