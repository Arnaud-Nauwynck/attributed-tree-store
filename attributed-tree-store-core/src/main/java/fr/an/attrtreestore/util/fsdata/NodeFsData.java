package fr.an.attrtreestore.util.fsdata;

import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.Getter;

@Getter
public abstract class NodeFsData {
	// NodeNamesPath path; //... may be huge in memory ... responsibility of caller to carry it in optimized structures
	public final NodeName name;

	public final long creationTime;

	public final long lastModifiedTime;

	public final ImmutableMap<String,Object> extraFsAttrs;
	
	// ------------------------------------------------------------------------
	
	public NodeFsData(NodeName name, long creationTime, long lastModifiedTime, ImmutableMap<String,Object> extraFsAttrs) {
		this.name = name;
		this.creationTime = creationTime;
		this.lastModifiedTime = lastModifiedTime;
		this.extraFsAttrs = extraFsAttrs;
	}

	// ------------------------------------------------------------------------

	public abstract void accept(NodeNamesPath path, NodeFsDataVisitor visitor);
	
	
	
	// ------------------------------------------------------------------------
	
	@Getter
	public static class FileNodeFsData extends NodeFsData {

		public final long fileLength;

		public FileNodeFsData(NodeName name, long creationTime, long lastModifiedTime, ImmutableMap<String,Object> extraFsAttrs, //
				long fileLength) {
			super(name, creationTime, lastModifiedTime, extraFsAttrs);
			this.fileLength = fileLength;
		}

		@Override
		public void accept(NodeNamesPath path, NodeFsDataVisitor visitor) {
			visitor.caseFile(path, this);
		}

	}

	// ------------------------------------------------------------------------
	
	@Getter
	public static class DirNodeFsData extends NodeFsData {
		
		// (may be immutable) ensured sorted + unique per name 
		public final TreeSet<NodeName> childNames;

		public DirNodeFsData(NodeName name, long creationTime, long lastModifiedTime, 
				ImmutableMap<String,Object> extraFsAttrs, //
				TreeSet<NodeName> childNames) {
			super(name, creationTime, lastModifiedTime, extraFsAttrs);
			this.childNames = childNames;
		}
		
		@Override
		public void accept(NodeNamesPath path, NodeFsDataVisitor visitor) {
			visitor.caseDir(path, this);
		}
		
	}

}
