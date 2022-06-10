package fr.an.attrtreestore.storage.impl;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.an.attrtreestore.api.IInMemCacheReadTreeData;
import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData;
import fr.an.attrtreestore.api.ROCached_TreeData.IndexedBlobStorageInitMode;
import fr.an.attrtreestore.api.TreeData;
import fr.an.attrtreestore.api.override.OverrideNodeData;
import fr.an.attrtreestore.api.override.OverrideNodeStatus;
import fr.an.attrtreestore.api.override.OverrideTreeData;
import fr.an.attrtreestore.impl.traversal.DefaultTreeDataCopier;
import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class PersistedTreeData extends TreeData implements IWriteTreeData, IInMemCacheReadTreeData {

	private static final String FILENAME_manifest = "manifest.json";

	@SuppressWarnings("unused")
	private final BlobStorage blobStorage;
	
	@SuppressWarnings("unused")
	private final String baseDirname;

	private final String manifestFilename;
	
	@SuppressWarnings("unused")
	private final AttrDataEncoderHelper attrDataEncoderHelper;
	private final IndexedBlobStorage_TreeNodeDataEncoder indexedTreeNodeDataEncoder;
			
	private ReadUnionOverrideLayer_TreeData unionTree;
	
	private Compound_OverrideTreeData underlyingOverrideTree;
	
	@SuppressWarnings("unused")
	private ROCached_TreeData underlyingReadIndexedTree;

	private ObjectMapper jsonMapper = new ObjectMapper(); 

	private final Object manifestLock = new Object();
	// @GuardedBy("manifestLock")
	private PersistedTreeDataManifest manifest;
	
	/**
	 * internal for persisting current file names, after recompacting indexes / rolling WAL 
	 */
	private static class PersistedTreeDataManifest {
		public static final String indexFilePrefix = "index-";
		public static final String indexFileSuffix = ".data";

		public static final String walOverrideFilePrefix = "override-";
		public static final String walOverrideFileSuffix = ".data";
		
		public int currIndexNum = -1;
		public String indexFilename;

		// public String backupIndexFilename;
		// public List<String> backupWalFilenames;
		
		public List<String> sequenceWalFilenames = new ArrayList<>(); 
		public int currWalNum = -1;
		
		private String indexFilename(int num) {
			return indexFilePrefix + num + indexFileSuffix;
		}
		public String newIndexFile() {
			val num = ++currIndexNum;
			val res = indexFilename(num);
			return res;
		}
		private String walOverrideFilename(int num) {
			return walOverrideFilePrefix + num + walOverrideFileSuffix;
		}
		public String addWalFile() {
			val num = ++currWalNum;
			val res = walOverrideFilename(num);
			sequenceWalFilenames.add(res);
			return res;
		}
	}

	// ------------------------------------------------------------------------
	
	public PersistedTreeData(BlobStorage blobStorage, String baseDirname,
			AttrDataEncoderHelper attrDataEncoderHelper
			) {
		this.blobStorage = blobStorage;
		this.baseDirname = baseDirname;
		this.attrDataEncoderHelper = attrDataEncoderHelper;
		this.indexedTreeNodeDataEncoder = new IndexedBlobStorage_TreeNodeDataEncoder(attrDataEncoderHelper);

		CachedROIndexedBlobStorage_TreeNodeData underlyingReadIndexedTree;
		List<WALBlobStorage_OverrideTreeData> sequenceOverrideTrees = new ArrayList<>();
		
		if (! blobStorage.exists(baseDirname)) {
		    log.info("dir '" + baseDirname + "' not exists for PersistedTreeData => mkdirs");
		    blobStorage.mkdirs(baseDirname);
		}
		this.manifestFilename = baseDirname + "/" + FILENAME_manifest;
		PersistedTreeDataManifest manifest;
		if (blobStorage.exists(manifestFilename)) {
			// reload filenames from manifest
			manifest = readManifest();

			IndexedBlobStorageInitMode initMode = IndexedBlobStorageInitMode.RELOAD_ROOT_ONLY;
			long initPrefetchSize = 10 * 1024*1024;
			String indexFile = baseDirname + "/" + manifest.indexFilename;
			underlyingReadIndexedTree = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, indexFile,
					indexedTreeNodeDataEncoder, initMode, initPrefetchSize);

			for(val walFilename: manifest.sequenceWalFilenames) {
				String walFile = baseDirname + "/" + walFilename;
				val underlyingOverrideTree = new WALBlobStorage_OverrideTreeData(blobStorage, walFile, attrDataEncoderHelper);
				underlyingOverrideTree.initReload();
				sequenceOverrideTrees.add(underlyingOverrideTree);
			}

		} else {
			// init default
			manifest = new PersistedTreeDataManifest();

			String indexFilename = manifest.newIndexFile();
			manifest.indexFilename = indexFilename;
			String indexFile = baseDirname + "/" + indexFilename;
			String walFilename = manifest.addWalFile();

			log.info("init empty persistedTree dir: '" + baseDirname + "' (manifest:" + manifestFilename + ", index:" + indexFilename + ", wal: " + walFilename + ")");
			
			new InMem_TreeData().recursiveWriteFull(blobStorage, indexFile, indexedTreeNodeDataEncoder);
			
			underlyingReadIndexedTree = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, indexFile,
					indexedTreeNodeDataEncoder, IndexedBlobStorageInitMode.INIT_EMPTY, 0);
			
			String walFile = baseDirname + "/" + walFilename;
			val underlyingOverrideTree = new WALBlobStorage_OverrideTreeData(blobStorage, walFile, attrDataEncoderHelper);
			underlyingOverrideTree.initCreateEmpty();
			sequenceOverrideTrees.add(underlyingOverrideTree);

			writeManifest(manifest);
		}
		
		this.underlyingReadIndexedTree = underlyingReadIndexedTree;
		this.underlyingOverrideTree = new Compound_OverrideTreeData(sequenceOverrideTrees);
		this.unionTree = new ReadUnionOverrideLayer_TreeData(underlyingReadIndexedTree, underlyingOverrideTree);
	}

	// implements IReadableTreeData
	// ------------------------------------------------------------------------
	
	@Override
	public NodeData get(NodeNamesPath path) {
		return unionTree.get(path);
	}
	
	@Override // implements IInMemCacheReadTreeData
	public OverrideNodeData getIfInMemCache(NodeNamesPath path) {
		OverrideNodeData overrideData;
		if (underlyingOverrideTree instanceof IInMemCacheReadTreeData) {
			overrideData = ((IInMemCacheReadTreeData) underlyingOverrideTree).getIfInMemCache(path);
		} else {
			overrideData = underlyingOverrideTree.getOverride(path);
		}
		if (overrideData != null) {
			switch(overrideData.status) {
			case DELETED: return OverrideNodeData.DELETED;
			case UPDATED: return overrideData;
			case NOT_OVERRIDEN: break; // cf next
			}
		}
		if (underlyingReadIndexedTree instanceof IInMemCacheReadTreeData) {
			return ((IInMemCacheReadTreeData) underlyingReadIndexedTree).getIfInMemCache(path);
		} else {
			// underlying tree does not support getIfInMemCache().. fallback
			NodeData nodeData = underlyingReadIndexedTree.get(path);
			if (nodeData == null) {
				return OverrideNodeData.DELETED;
			} else {
				return new OverrideNodeData(OverrideNodeStatus.UPDATED, nodeData);
			}
		}
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
	
	@AllArgsConstructor
	protected static class RollWALResult {
		String newWalFilename;
		WALBlobStorage_OverrideTreeData newWalOverrideTree;
		OverrideTreeData[] prevWalOverrideTrees;
	}
	
	protected RollWALResult rollAddWal() {
		synchronized(manifestLock) {
			val newWalFilename = manifest.addWalFile();
			String walFile = baseDirname + "/" + newWalFilename;
			val newWalOverrideTree = new WALBlobStorage_OverrideTreeData(blobStorage, walFile, attrDataEncoderHelper);
			newWalOverrideTree.initCreateEmpty();
			writeManifest(manifest);
	
			newWalOverrideTree.startWrite();
			
			// atomic add roll wal
			val prevWalOverrideTrees = underlyingOverrideTree.rollAddWal(newWalOverrideTree);
			
			val prevWal = (WALBlobStorage_OverrideTreeData) prevWalOverrideTrees[prevWalOverrideTrees.length-1];
			prevWal.flushStopWrite();
			
			return new RollWALResult(newWalFilename, newWalOverrideTree, prevWalOverrideTrees);
		}
	}
	
	public void writeRecompactIndexFile() {
		synchronized(manifestLock) {
			// rolling WAL, take snapshot
			RollWALResult rollWALResult = rollAddWal();
			val prevSnaphostOverrideTree = new Compound_OverrideTreeData(rollWALResult.prevWalOverrideTrees);
			val prevSnapshotTree = new ReadUnionOverrideLayer_TreeData(underlyingReadIndexedTree, prevSnaphostOverrideTree);
			// copy snapshot to in-memory tree
			val newIndexingTree = new InMem_TreeData();
			DefaultTreeDataCopier.copy(prevSnapshotTree, newIndexingTree);
			// re-index and save file
			val prevIndexFilename = manifest.indexFilename;
			val newIndexFilename = manifest.newIndexFile();
			val newIndexFile = baseDirname + "/" + newIndexFilename;
			newIndexingTree.recursiveWriteFull(blobStorage, newIndexFile, indexedTreeNodeDataEncoder);
			
			// check reload all (check for data-integrity + preload fetching..)
			IndexedBlobStorageInitMode initMode = IndexedBlobStorageInitMode.RELOAD_ROOT_ONLY;
			long initPrefetchSize = 10 * 1024*1024;
			val newUnderlyingReadIndexedTree = new CachedROIndexedBlobStorage_TreeNodeData(blobStorage, newIndexFile,
					indexedTreeNodeDataEncoder, initMode, initPrefetchSize);
			
			// switch index filename + remove intermediate wal filename
			manifest.indexFilename = newIndexFilename; 
			manifest.sequenceWalFilenames = new ArrayList<>(); 
			manifest.sequenceWalFilenames.add(rollWALResult.newWalFilename);
			
			writeManifest(manifest);

			// TOADD delete previous backup, rename curr to backup?
			blobStorage.deleteFile(baseDirname + "/" + prevIndexFilename);
			
			writeManifest(manifest);
			
			// also change in-memory (not strictly needed, for memory compaction)
			this.underlyingOverrideTree = new Compound_OverrideTreeData(new OverrideTreeData[] { rollWALResult.newWalOverrideTree });
			this.underlyingReadIndexedTree = newUnderlyingReadIndexedTree;
			this.unionTree = new ReadUnionOverrideLayer_TreeData(underlyingReadIndexedTree, underlyingOverrideTree);

		}
	}


	private PersistedTreeDataManifest readManifest() {
		val manifestFileContent = blobStorage.readFile(manifestFilename);
		try {
			return jsonMapper.readValue(manifestFileContent, PersistedTreeDataManifest.class);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read " + manifestFilename, ex);
		}
	}

	private void writeManifest(PersistedTreeDataManifest manifest) {
		byte[] manifestFileContent;
		try {
			manifestFileContent = jsonMapper.writeValueAsBytes(manifest);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write " + manifestFilename, ex);
		}
		blobStorage.writeFile(manifestFilename, manifestFileContent);
	}

}
