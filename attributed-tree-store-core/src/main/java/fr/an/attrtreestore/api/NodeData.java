package fr.an.attrtreestore.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * read-only data for a node with attributes and child names
 * 
 * ( only internal fields for LRU and lastQueryTimestamp are mutable ?)
 */
@Getter
@AllArgsConstructor
@RequiredArgsConstructor
@Builder
public class NodeData {
	
	public final NodeName name;

	/** use-defined type... example: file / node / .. */
	public final int type;

	public final int mask;

	public final ImmutableSet<NodeName> childNames; 

	public int childCount() {
		return (childNames != null)? childNames.size() : 0;
	}

	public final ImmutableMap<String,NodeAttr> attrs;

	public int attrCount() {
		return (attrs != null)? attrs.size() : 0;
	}

	/** external creationTime from backend if any */
	public final long externalCreationTime;
	
	/** external lastModifiedTime from backend if any */
	public final long externalLastModifiedTime;

	/** external fileLength from backend if any, or user-defined value */
	public final long externalLength;

	/** System.currentTimeMillis() of the jvm that queried the external backend the last time  
	 * can be modified without persisting... transient 
	 */
	private transient long lastExternalRefreshTimeMillis;

	/** internal System.currentTime() of the jvm that modified this path NodeData the last time */
	public final long lastTreeDataUpdateTimeMillis;

	/** internal incremented counter when modifying this path NodeData the last time */
	public final int lastTreeDataUpdateCount;

	
	/** internal mask for update recomputation propagations 
	 * (can/)might be modified without persisting .. transient
	 */
	@Getter
	private transient int treeDataRecomputationMask;

	
	
	/** internal counter of the jvm that queried this path NodeData 
	 * can be modified without persisting... transient 
	 */
	@Getter
	private transient int lruCount;
	
	/** internal exponentially amortized counter  
	 * can be modified without persisting... transient 
	 */
	@Getter
	private transient int lruAmortizedCount;
	
	/** internal System.currentTime() of the jvm that queried this path NodeData the last time 
	 * can be modified without persisting... transient 
	 */
	@Getter
	private transient long lastTreeDataQueryTimeMillis;

	// ------------------------------------------------------------------------

	public void setTreeDataRecomputationMask(int treeDataRecomputationMask) {
		this.treeDataRecomputationMask = treeDataRecomputationMask;
	}

	public void incrLruCount() {
		this.lruCount++;  // TODO... need atomic cas
	}
	
	public void setLruCount(int lruCount) {
		this.lruCount = lruCount;
	}

	public void setLruAmortizedCount(int lruAmortizedCount) {
		this.lruAmortizedCount = lruAmortizedCount;
	}

	public void setLastTreeDataQueryTimeMillis(long lastTreeDataQueryTimeMillis) {
		this.lastTreeDataQueryTimeMillis = lastTreeDataQueryTimeMillis;
	}


	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return name.toText() + " (type: " + type //
				+ ((childNames.size() > 0)? ", " + childNames.size() + " child" : "")
				+ ")";
	}
	
	public boolean isSimpleFieldsEquals(NodeData other) {
		return type == other.type && 
				mask == other.mask && 
				externalCreationTime == other.externalCreationTime &&
				externalLastModifiedTime == other.externalLastModifiedTime &&
				externalLength == other.externalLength;
	}


}
