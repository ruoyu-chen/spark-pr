/**
 * 
 */
package cn.edu.bistu.spark.pr;

import java.io.Serializable;

/**
 * @author chenruoyu
 *
 */
public class Node implements Serializable {
	private static final long serialVersionUID = -8385191780501513828L;
	private long srcId;
	private boolean isDanglingNode = false;//是否汇点
	private long[] destIds;
	public Node(long srcId, long[] destIds){
		this.srcId = srcId;
		this.destIds = destIds;
		if(destIds ==null||destIds.length==0){
			this.isDanglingNode = true;
		}
	}
	public long getSrcId() {
		return srcId;
	}
	public long[] getDestIds() {
		return destIds;
	}
	public boolean isDanglingNode() {
		return isDanglingNode;
	}
}
