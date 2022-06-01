package fr.an.attrtreestore.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.AllArgsConstructor;
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
public class NodeData {
	
	public final NodeName name;

	/** use-defined type... example: file / node / .. */
	public final int type;

	public final int mask;

	public final ImmutableSet<NodeName> childNames; 
	
	public final ImmutableMap<String,NodeAttr> attrs;

	public final long creationTime;
	
	public final long lastModifiedTime;
	
	/** use-defined field1Long: in case (frequent) this node represent a file... the fileLength */
	public final long field1Long;
	
	@Getter
	private final long lastModifTimestamp;

	
	@Getter
	private int lruCount;
	@Getter
	private int lruAmortizedCount;
	@Getter
	private long lastQueryTimestamp;


	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return name.toText() + " (type: " + type //
				+ ((childNames.size() > 0)? ", " + childNames.size() + " child" : "")
				+ ")";
	}
	
}
