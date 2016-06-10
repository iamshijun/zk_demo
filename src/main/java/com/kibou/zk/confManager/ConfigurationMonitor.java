package com.kibou.zk.confManager;

import java.util.Arrays;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ConfigurationMonitor implements Watcher, DataCallback {

	private ZooKeeper zk;
	private String znode;
	private byte[] preData;
	
	private ConfigurationChangedListener listener;

	public ConfigurationMonitor(ZooKeeper zk, String znode,
			ConfigurationChangedListener listener) throws KeeperException,
			InterruptedException {
		this.zk = zk;
		this.znode = znode;
		this.listener = listener;

		zk.getData(znode, true, this, null);
	}

	public static interface ConfigurationChangedListener {
		void handleChange(byte[] data);
		void closing();
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		//boolean dead = false;
		if (event.getType() == Event.EventType.None) {
			// We are are being told that the state of the
			// connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// In this particular example we don't need to do anything
				// here - watches are automatically re-registered with
				// server and any watches triggered while the client was
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				listener.closing();
				break;
			default:
				break;
			}
		} else {
			if(path != null && path.equals(znode))
				zk.getData(znode, true, this, null);
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data,
			Stat stat) {
		Code reasonCode = Code.get(rc);
		switch (reasonCode) {
		case OK:
			break;
//		case AUTHFAILED:
//		case BADVERSION:
//		case MARSHALLINGERROR:
//			return;
		case NONODE:
			//node no exist
			break;
		case SESSIONEXPIRED:
		case NOAUTH:
			//dead
			return;
		default:
			zk.getData(znode, true, this, null);//retry
			break;
		}
		
		if((data == null && data != preData)
				|| (data != null && !Arrays.equals(data, preData))){
			listener.handleChange(data);
			preData = data;
		}
	}

}