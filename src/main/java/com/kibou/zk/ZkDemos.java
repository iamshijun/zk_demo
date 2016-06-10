package com.kibou.zk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kibou.zk.util.ZookeeperClientFactory;

public class ZkDemos {
	
	public static void main(String[] args) throws Throwable {
//		Thread thread = new Thread(new ConfigurationWatcher());
//		thread.start();
		
		ZooKeeper zk = null;
		try{
			zk =  ZookeeperClientFactory.newZooKeeper(ZookeeperCfgConstants.CONNECTSTRING,
					ZookeeperCfgConstants.SESSION_TIMEOUT);
			
			//Thread.sleep(5000);
			//String data = "{\"time\":1,\"path\":\"F:/conf/AbleskyConfig.properties\"}";
			//zk.setData("/conf/AbleskyConfig", data.getBytes(), -1);
			File file = new File("F:/conf/AbleskyConfig.properties");
			FileInputStream fis = new FileInputStream(file);
			byte[] data = new byte[fis.available()];
			fis.read(data);
			zk.setData("/conf/AbleskyConfig", data, -1);
			
			fis.close();
		}catch(Throwable t){
			throw t;
		}finally{
			if(zk != null){
				zk.close();
			}
		}
		
//		thread.join();
	}
	
	static class ConfigurationWatcher implements Runnable,Watcher{

		private final ZooKeeper zk;
		private CountDownLatch countDownLatch;
		
		public  ConfigurationWatcher() throws IOException {
			zk =  ZookeeperClientFactory.newZooKeeper(ZookeeperCfgConstants.CONNECTSTRING,
					ZookeeperCfgConstants.SESSION_TIMEOUT);
			
			countDownLatch = new CountDownLatch(1);
		}
		
		@Override
		public void process(WatchedEvent event) {
			if(event.getType() == EventType.NodeDataChanged){
				try {
					byte[] newdata = zk.getData("/conf/AbleskyConfig",false,null);//同步获取data
					System.out.println("new data :\n" + new String(newdata));
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				countDownLatch.countDown();
			}
			
		}
		
		@Override
		public void run() {
			final Object context = new Object(); //context
			zk.getData("/conf/AbleskyConfig", this ,new DataCallback() { // DataCallback实现了 AsyncCallback,这里是一个异步获取的方式获取data
				@Override
				public void processResult(int rc, String path, Object ctx, byte[] data,
						Stat stat) {
					System.out.println("old data :\n" + new String(data));
					System.out.println(ctx == context);
				}
			}, context);
			
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
}
