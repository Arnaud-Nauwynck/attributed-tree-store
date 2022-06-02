package fr.an.attrtreestore.storage.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import fr.an.attrtreestore.api.NodeAttr;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;

public class TreeDataTstGenerator {

	private long clockBackendStorageTime = 1L; // emulate clock for backend 
	private long clockMillis = 1L;
	private int modifCounter = 1;
	

	public NodeData createDirData(NodeName name, ImmutableSet<NodeName> childNames) {
		return new NodeData(name,
			1, // type;
			0, // mask;
			childNames, 
			ImmutableMap.<String,NodeAttr>of(), // attrs
			clockBackendStorageTime++, // externalCreationTime, 
			clockBackendStorageTime++, // externalLastModifiedTime,
			0L, // externalLength;
			0L, // lastExternalRefreshTimeMillis
			clockMillis++, // lastTreeDataUpdateTimeMillis
			modifCounter++, // lastTreeDataUpdateCount
			0, 0, 0, 0L); // treeDataRecomputationMask, lruCount, lruAmortizedCount, lastQueryTimestamp
	}

}
