package com.kibou.zk.confManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.kibou.zk.ZookeeperCfgConstants;
import com.kibou.zk.util.ZooKeeperPathHelper;

public class ConfigurationPublisher{
	
	private static String defaultDataNode 			= "/conf/WebSiteConfig";
	private static String defaultOutput 		    = "/conf/system.properties";
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		String connectString = ZookeeperCfgConstants.CONNECTSTRING;
		int sessionTimeout = ZookeeperCfgConstants.SESSION_TIMEOUT;
		String znode = defaultDataNode;
		String filename = defaultOutput;
		
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
			filename = args[3];
		}	
		
		boolean waitForClients = true;
		new ConfigurationPublisher(connectString,sessionTimeout,znode,filename)
				.publish(waitForClients);
	}
	
	
	private ZkClient zkClient;
	private String znode;
	
	private File file;//TODO support directory,pulish all the subdi's configuration files;
	
	public ConfigurationPublisher(String zkServers,int ioTimeout,String znode,String filename)
			throws IOException {
		this.zkClient = new ZkClient(zkServers, ioTimeout,ioTimeout,new BytesPushThroughSerializer());
		this.znode = znode;
		
		this.file = new File(filename);
		if(!file.exists())
			throw new FileNotFoundException(file.getCanonicalPath() + " cannot be found");
	}

	public void publish(boolean waitForClients) {
		
		byte[] fileData;
		String checkSum;
		try (FileInputStream fis = new FileInputStream(file);) {
			fileData = new byte[fis.available()];
			fis.read(fileData);
			//checkSum = DigestUtils.md5Hex(fis);
			checkSum = DigestUtils.md5Hex(fileData);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		String checksumNode = ZooKeeperPathHelper.child(znode, "checksum");
		String dataNode = ZooKeeperPathHelper.child(znode, "data");
		
		Optional<byte[]> checkSumOptinal = Optional.fromNullable(zkClient.<byte[]>readData(checksumNode,true));
		if(!checkSumOptinal.isPresent() || !Objects.equal(new String(checkSumOptinal.get()), checkSum)){
			/*try{
				zkClient.createPersistent(dataNode, true);
			}catch(ZkNodeExistsException ignored){}
			try{
				zkClient.createPersistent(checksumNode, true);
			}catch(ZkNodeExistsException ignored){}*/ //事前初始化好!
			
			zkClient.writeData(dataNode, fileData);//replace the old config data
			zkClient.writeData(checksumNode, checkSum.getBytes());//rewrite the checksum
			
			if(waitForClients)
				doWaitForClient();
			
		}else{
			System.out.println("Configuration got not changed");
		}
	}

	private void doWaitForClient() {
		String clientsNode = ZooKeeperPathHelper.child(znode, "clients");
		final List<String> subscribeConfClients = zkClient.getChildren(clientsNode);
		if(subscribeConfClients.size() == 0)
			return;
		final CountDownLatch cdl = new CountDownLatch(1);
		String reachNode = ZooKeeperPathHelper.child(znode, "reach");
		
		List<String> currentChilds = zkClient.subscribeChildChanges(reachNode, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				if(currentChilds.size() == subscribeConfClients.size()){
					System.out.println("All Clients have reported");
					cdl.countDown();
				}
			}
		});
		
		if(currentChilds.size() != subscribeConfClients.size()){
			try {
				cdl.await();
			} catch (InterruptedException ignored) {
				ignored.printStackTrace();
			}
		}
		//暂时认定 clients和reach中的列表数据是相等的
		for(String child : subscribeConfClients){
			zkClient.delete(ZooKeeperPathHelper.child(reachNode,child));
		}
	}
}
