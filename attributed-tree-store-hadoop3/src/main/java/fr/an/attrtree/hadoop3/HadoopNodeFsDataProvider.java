package fr.an.attrtree.hadoop3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.cachedfsview.PrefetchNodeFsDataContext;
import fr.an.attrtreestore.util.LoggingCallStats;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * implementation of NodeFsDataProvider delegating to Hadoop FileSystem
 */
@Slf4j
public class HadoopNodeFsDataProvider extends NodeFsDataProvider {

	@Getter
	final String baseUrl;

	@Getter
	private final FileSystem fs;
	
	@Getter
	private final Path basePath;
	
	private final NodeNameEncoder nodeNameEncoder;
	
	private final LoggingCallStats fsGetStatusStats;
	private final LoggingCallStats fsListStatusStats;
	
	// ------------------------------------------------------------------------
	
	public HadoopNodeFsDataProvider(String baseUrl, FileSystem fs,
			NodeNameEncoder nodeNameEncoder) {
		this.baseUrl = baseUrl;
		this.fs = fs;
		this.basePath = new Path(baseUrl);
		this.nodeNameEncoder = nodeNameEncoder;
		this.fsGetStatusStats = new LoggingCallStats("HadoopFS " + baseUrl + " .getStatus()", "getStatus", 1000);
		this.fsListStatusStats = new LoggingCallStats("HadoopFS " + baseUrl + " .listStatus()", "listStatus", 1000);
	}

	// ------------------------------------------------------------------------

	@Override
	public NodeFsData queryNodeFsData(NodeNamesPath subpath, PrefetchNodeFsDataContext prefetchCtx) {
		Path hadoopPath = subpathToHadoopPath(subpath);
		
		FileStatus hadoopFileStatus = fsGetFileStatus(hadoopPath);
		if (hadoopFileStatus == null) {
			return null;
		}
		val name = subpath.lastName();
		
		long creationTime = hadoopFileStatus.getModificationTime(); // info not known in hadoop.. use modificationTime?
		long lastModifiedTime = hadoopFileStatus.getModificationTime();
		ImmutableMap<String,Object> extraFsAttrs = ImmutableMap.of(
				// TOADD..
				);
		
		NodeFsData res;
		if (hadoopFileStatus.isDirectory()) {
			// also query child files
			FileStatus[] hadoopChildLs = fsListStatus(hadoopPath);
			val childNames = new TreeSet<NodeName>();
			for(val hadoopChild: hadoopChildLs) {
				val childFileName = hadoopChild.getPath().getName();
				val childName = nodeNameEncoder.encode(childFileName);
				childNames.add(childName);
			}
			res = new DirNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, childNames);
		} else if (hadoopFileStatus.isFile()) {
			long fileLength = hadoopFileStatus.getLen();
			res = new FileNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, fileLength);
		} else if (hadoopFileStatus.isSymlink()) {
			log.info("ignore Hadoop symlink");
			res = null;
		} else {
			log.error("should not occur");
			res = null;
		}
		return res;
	}

	// ------------------------------------------------------------------------
	
	protected Path subpathToHadoopPath(NodeNamesPath subpath) {
		String subPathSlash = subpath.toPathSlash(); // example: "a/b" for subdir inside baseUrl "hdfs:///XX/yy"
		return new Path(basePath, subPathSlash); // example: "hdfs:///XX/yy/a/b"
	}
	
	protected FileStatus fsGetFileStatus(Path hadoopPath) {
		FileStatus res;
		long startTime = System.currentTimeMillis();
		try {
			res = fs.getFileStatus(hadoopPath);
		} catch(FileNotFoundException ex) {
			long millis = System.currentTimeMillis() - startTime;
			fsGetStatusStats.incrLog(millis);
			return null;
		} catch (IOException ex) {
			long millis = System.currentTimeMillis() - startTime;
			fsGetStatusStats.incrLogFailed(millis, ex);
			throw new RuntimeException("Failed", ex);
		}
		long millis = System.currentTimeMillis() - startTime;
		fsGetStatusStats.incrLog(millis);
		
		return res;
	}

	protected FileStatus[] fsListStatus(Path hadoopPath) {
		FileStatus[] res;
		long startTime = System.currentTimeMillis();
		try {
			res = fs.listStatus(hadoopPath);
		} catch(FileNotFoundException ex) {
			// should not occur (query just before)
			long millis = System.currentTimeMillis() - startTime;
			fsListStatusStats.incrLogFailed(millis, ex);
			return null;
		} catch (IOException ex) {
			long millis = System.currentTimeMillis() - startTime;
			fsListStatusStats.incrLogFailed(millis, ex);
			throw new RuntimeException("Failed", ex);
		}
		long millis = System.currentTimeMillis() - startTime;
		fsListStatusStats.incrLog(millis);
		return res;
	}
	
}
