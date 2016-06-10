package com.kibou.zk.coordinate;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import static com.kibou.zk.ZookeeperCfgConstants.*;

public class ZkSimpleLockDemo1 implements Runnable{

	public static void main(String[] args) {
		new ZkSimpleLockDemo1().run();
	}
	
	public void run(){
		int threadCount = 5;
		CyclicBarrier cb = new CyclicBarrier(threadCount + 1);
		
		Thread[] threads = new Thread[threadCount];
		for(int i = 0; i < threadCount; ++i){
			threads[i] = new ZkCli(cb);
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
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	class ZkCli extends Thread {

		private final CyclicBarrier cycliBarrier;
		
		public ZkCli(CyclicBarrier cycliBarrier){
			this.cycliBarrier = cycliBarrier;
		}
		
		@Override
		public void run() {
			ZooKeeper zk = null;
			try {
				zk = new ZooKeeper(CONNECTSTRING, 3000, null);
				
				cycliBarrier.await();
				
				zk.create("/lock/simpleLock", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				
				System.out.println(Thread.currentThread() + " got the lock successfully");
			} catch (IOException | BrokenBarrierException e) {
				e.printStackTrace();
			} catch (InterruptedException | KeeperException e) {
				if(e instanceof NodeExistsException){
					System.out.println(Thread.currentThread() + " failed to get the lock");
				}else{
					e.printStackTrace();
				}
			} finally {
				if (zk != null) {
					try {
						zk.close();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
