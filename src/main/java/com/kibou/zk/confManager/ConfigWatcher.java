package com.kibou.zk.confManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.util.ZooKeeperPathHelper;

public class ConfigWatcher implements Runnable, Watcher,
			ConfigurationMonitor.ConfigurationChangedListener {

	private static String defaultZnode 			= "/conf/WebSiteConfig/data";
	private static String defaultOutput 		= "/conf/system_slice.properties";
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		String connectString = ZookeeperCfgConstants.CONNECTSTRING;
		int sessionTimeout = ZookeeperCfgConstants.SESSION_TIMEOUT;
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
		
		final ConfigWatcher configWatcher = 
				new ConfigWatcher(connectString, sessionTimeout, znode, syncFile);
		
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
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		while(!dead){
			synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
	
	private final ZooKeeper zk;
	private File syncFile;
	private String znode;
	private ConfigurationMonitor cm;
	
	private String localHostAddress;
	
	public ConfigWatcher(String connectString,int sessionTimeout,String znode,String filename,String localHostAddress) 
			throws IOException, KeeperException, InterruptedException{
		this.znode = znode;
		this.syncFile = prepareSyncFile(filename);
		
		this.zk = new ZooKeeper(connectString, sessionTimeout, this);
		//ConfigurationMonitor负责监控节点变化(期间发生的事件的处理)
		this.cm = new ConfigurationMonitor(zk, znode,this);
		
		if(localHostAddress == null){
			InetAddress localHost  = Inet4Address.getLocalHost();
			localHostAddress = localHost.getHostAddress();
		}
		this.localHostAddress = localHostAddress;
	}
	
	public ConfigWatcher(String connectString,int sessionTimeout,String znode,String filename) 
			throws IOException, KeeperException, InterruptedException{
		this(connectString, sessionTimeout, znode, filename,null);
	}
	
	private File prepareSyncFile(String filename){
		File file = new File(filename);
		File parentFile = file.getParentFile();
		if(!parentFile.exists() && parentFile.mkdirs()){
			throw new IllegalStateException("Failed to mkdir parent directory for" + filename);
		}
	    /*if(file.exists() && (!file.isFile() || !file.canRead())){
			throw new IllegalArgumentException(filename + "is not a validate file or cannot be read");
		}*/
		
		return file;
	}
	
	@Override
	public void process(WatchedEvent event) {
		cm.process(event);
	}

	@Override
	public void handleChange(byte[] data) {
		try (FileOutputStream fos  = new FileOutputStream(syncFile)) {
			fos.write(data);
			
			try {
				String reach = ZooKeeperPathHelper.sibling(znode, "reach");
				zk.create(reach + "/" + localHostAddress, "".getBytes(), 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException ignored) {
			//ignore this,cause it was checked before
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
