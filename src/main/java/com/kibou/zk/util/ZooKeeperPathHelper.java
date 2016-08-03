package com.kibou.zk.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZooKeeperPathHelper {
	
	public static byte[] emptyData(){
		return new byte[0];
	}
	
	public static int seqOfPath(String path,String prefix){
		String seq = path.substring(prefix.length()+1);
		try{//假设没有恶意或者程序错误 seq 都是可转成数值的
			return Integer.parseInt(seq);
		}catch(NumberFormatException nfe){
			nfe.printStackTrace();//oh no! forget about this!
			return Integer.MAX_VALUE;
		}
	}
	
	public static List<Integer> nodeSequences(List<String> children,String nodeName){
		if(children == null || children.size() == 0 ){
			return Collections.emptyList();
		}
		List<Integer> sequences = new ArrayList<>();
		for(String child : children){
			if(child.equals(nodeName))
				continue;
			sequences.add(seqOfPath(child,nodeName));
		}
		//Collections.sort(sequences);
		return sequences;
	}
	
	public static String sibling(String znode,String siblingName){
		int lastIndexOf = znode.lastIndexOf("/");
		if(lastIndexOf == 0){
			return siblingName.startsWith("/") ?  siblingName : "/" + siblingName; 
		}
		return znode.substring(0,siblingName.startsWith("/") ? lastIndexOf : lastIndexOf+1) + siblingName;
	}
	
	public static String child(String parentNode,String childNode){
		if(childNode.startsWith(parentNode))
			return childNode;
		return parentNode + (childNode.startsWith("/") ? childNode : "/" + childNode);  
	}
	
	public static String buildPath(String p1,String p2,String p3){
		return (p1 + "/" + p2).intern() + "/" + p3;
	}
	
	public static void main(String[] args) {
		System.out.println(sibling("/a/b/c", "/d"));
	}
}
