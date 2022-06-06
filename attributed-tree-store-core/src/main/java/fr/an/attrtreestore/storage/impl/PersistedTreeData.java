package fr.an.attrtreestore.storage.impl;

import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import lombok.val;

/**
 * 
 */
public class PersistedTreeData extends TreeData implements IWriteTreeData {

	private static final String FILENAME_manifest = "manifest.json";

	@SuppressWarnings("unused")
	private final BlobStorage blobStorage;
	@SuppressWarnings("unused")
	private final String baseDirname;

	@SuppressWarnings("unused")
	private final AttrDataEncoderHelper attrDataEncoderHelper;
	
	private ReadUnionOverrideLayer_TreeData unionTree;
	
	private WALBlobStorage_OverrideTreeData underlyingOverrideTree;
	
	private CachedROIndexedBlobStorage_TreeNodeData underlyingReadIndexedTree;

	private ObjectMapper jsonMapper = new ObjectMapper(); 

	private static class PersistedTreeDataManifest implements Serializable {

		/** */
		private static final long serialVersionUID = 1L;
		
		public String underlyingReadFilename = "readonly.data";
		public String walOverrideFilename = "override.data";
	
	}

	// ------------------------------------------------------------------------
	
	public PersistedTreeData(BlobStorage blobStorage, String baseDirname,
			AttrDataEncoderHelper attrDataEncoderHelper
			) {
		this.blobStorage = blobStorage;
		this.baseDirname = baseDirname;
		this.attrDataEncoderHelper = attrDataEncoderHelper;
		val indexedTreeNodeDataEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoderHelper);

		if (blobStorage.exists(baseDirname)) {
			blobStorage.mkdirs(baseDirname);
		}
		String manifestFile = baseDirname + "/" + FILENAME_manifest;
		PersistedTreeDataManifest manifest;
		if (blobStorage.exists(manifestFile)) {
			val manifestFileContent = blobStorage.readFile(manifestFile);
			try {
				manifest = jsonMapper.readValue(manifestFileContent, PersistedTreeDataManifest.class);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to read " + manifestFile, ex);
			}
		} else {
			// init default
			manifest = new PersistedTreeDataManifest();
			
			byte[] manifestFileContent;
			try {
				manifestFileContent = jsonMapper.writeValueAsBytes(manifest);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write " + manifestFile, ex);
			}
			blobStorage.writeFile(manifestFile, manifestFileContent);
		}
		
		
		IndexedBlobStorageInitMode initMode = IndexedBlobStorageInitMode.RELOAD_ROOT_ONLY;
		long initPrefetchSize = 10 * 1024*1024;
		String readFilename = baseDirname + "/" + manifest.underlyingReadFilename;
		this.underlyingReadIndexedTree = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, readFilename,
				indexedTreeNodeDataEncoder, initMode, initPrefetchSize);
		
		String walOverrideFile = baseDirname + "/" + manifest.walOverrideFilename;
		this.underlyingOverrideTree = new WALBlobStorage_OverrideTreeData(blobStorage, walOverrideFile,
				attrDataEncoderHelper);
		
		this.underlyingOverrideTree.initCreateEmptyOrReload();
		
		this.unionTree = new ReadUnionOverrideLayer_TreeData(underlyingReadIndexedTree, underlyingOverrideTree);
	}

	// implements IReadableTreeData
	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		return unionTree.get(path);
	}
	

	// implements IWriteableTreeData
	// ------------------------------------------------------------------------
	
	@Override
	public void put(NodeNamesPath path, NodeData data) {
		unionTree.put(path, data);
	}

	@Override
	public void remove(NodeNamesPath path) {
		unionTree.remove(path);
	}

	// ------------------------------------------------------------------------
	
}
