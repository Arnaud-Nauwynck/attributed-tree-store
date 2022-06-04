package fr.an.attrtreestore.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

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

	/** use-defined type... example: dir:1 / file:2  .. */
	public static final int TYPE_DIR = 1;
	public static final int TYPE_FILE = 2;
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
	@Getter @Setter
	private transient long lastExternalRefreshTimeMillis;

	/** internal System.currentTime() of the jvm that modified this path NodeData the last time */
	public final long lastTreeDataUpdateTimeMillis;

	/** internal incremented counter when modifying this path NodeData the last time */
	public final int lastTreeDataUpdateCount;

	
	/** internal mask for update recomputation propagations 
	 * (can/)might be modified without persisting .. transient
	 */
	@Getter @Setter
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
	@Getter @Setter
	private transient long lastTreeDataQueryTimeMillis;

	// ------------------------------------------------------------------------

	public void setTreeDataRecomputationMask(int treeDataRecomputationMask) {
		this.treeDataRecomputationMask = treeDataRecomputationMask;
	}

	public void setLruCountAndAmortized(int lruCount, int lruAmortizedCount) {
		this.lruCount = lruCount;
		this.lruAmortizedCount = lruAmortizedCount;
	}

	public void incrUsed(long millis) {
		this.lruCount++;  // might need atomic cas, but ok if count not exact
		this.lastTreeDataQueryTimeMillis = millis;
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
