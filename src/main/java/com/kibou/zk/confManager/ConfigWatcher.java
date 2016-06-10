package com.kibou.zk.confManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConfigWatcher implements Runnable, Watcher,
			ConfigurationMonitor.ConfigurationChangedListener {

	private static int    defaultSessionTimeout = 6000;
	private static String defaultConnectString  = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
	private static String defaultZnode 			= "/conf/AbleskyConfig";
	private static String defaultOutput 		= "F:/conf/AC.properties";
	
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		String connectString = defaultConnectString;
		int sessionTimeout = defaultSessionTimeout;
		String znode = defaultZnode;
		String syncFile = defaultOutput;
		
		if(args.length >= 1){
			connectString = args[0];
		}
		if(args.length >= 2){
			sessionTimeout = Integer.parseInt(args[1]);
		}
		if(args.length >= 3){
			znode = args[2];
		}
		if(args.length >=4){
			syncFile = args[3];
		}
		
		final ConfigWatcher configWatcher = new ConfigWatcher(connectString, sessionTimeout, znode, syncFile);
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				configWatcher.closing();
			}
		});
		
		configWatcher.run();
	}
	
	
	volatile boolean dead = false; 
	
	@Override
	public void closing() {
		dead = true;
		synchronized (this) {
			notify();
		}
	}
	
	@Override
	public void run() {
		while(!dead){
			synchronized (cm) {
				try {
					wait();
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
	
	private ZooKeeper zk;
	private File syncFile;
//	private String znode;
	private ConfigurationMonitor cm;
	
	public ConfigWatcher(String connectString,int sessionTimeout,String znode,String filename) 
			throws IOException, KeeperException, InterruptedException{
//		this.znode = znode;
		this.syncFile = new File(filename);
		if(!syncFile.exists()){
			throw new FileNotFoundException(filename);
		}
		if(!syncFile.isFile() || !syncFile.canRead()){
			//throw new FileAccessFailedException();
		}
		
		this.zk = new ZooKeeper(connectString, sessionTimeout, this);
		this.cm = new ConfigurationMonitor(zk, znode,this);
	}
	
	@Override
	public void process(WatchedEvent event) {
		cm.process(event);
	}

	@Override
	public void handleChange(byte[] data) {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(syncFile);
			fos.write(data);
		} catch (FileNotFoundException ignored) {
			//ignore this,cause it was checked before
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fos != null){
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
