package com.kibou.zk.coordinate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class ZookeeperHelper {
	
	public static byte[] emptyData(){
		return new byte[0];
	}
	
	public static int getPathSeq(String path,String prefix){
		String seq = path.substring(prefix.length()+1);
		try{//假设没有恶意或者程序错误 seq 都是可转成数值的
			return Integer.parseInt(seq);
		}catch(NumberFormatException nfe){
			nfe.printStackTrace();//oh no! forget about this!
			return Integer.MAX_VALUE;
		}
	}
	
	public static List<Integer> getNodeSequences(List<String> children,String nodeName){
		if(children == null || children.size() == 0 ){
			return Collections.emptyList();
		}
		List<Integer> sequences = new ArrayList<>();
		for(String child : children){
			if(child.equals(nodeName))
				continue;
			sequences.add(getPathSeq(child,nodeName));
		}
		//Collections.sort(sequences);
		return sequences;
	}
}
