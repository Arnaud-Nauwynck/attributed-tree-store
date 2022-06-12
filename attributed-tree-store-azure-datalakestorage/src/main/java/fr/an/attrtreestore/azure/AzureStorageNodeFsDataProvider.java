package fr.an.attrtreestore.azure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.api.readprefetch.PrefetchNodeDataContext;
import fr.an.attrtreestore.api.readprefetch.PrefetchProposedPathItem;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.cachedfsview.PrefetchNodeFsDataContext;
import fr.an.attrtreestore.impl.name.DefaultNodeNameEncoderOptions;
import fr.an.attrtreestore.util.LoggingCounter;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureStorageNodeFsDataProvider extends NodeFsDataProvider {

    @Getter
    private final String displayName;
    @Getter
    private final String displayBaseUrl;
    
    // protected final AzSpnIdentityParams identityParam; ... implicit from fileSystemClient and baseDirClient  
    private final DataLakeFileSystemClient fileSystemClient;
    private final DataLakeDirectoryClient baseDirClient;

    private final String fsSubDirPath;
    
    private final NodeNameEncoder nodeNameEncoder;
    
    protected final LoggingCounter azPathGetProps_Counter = new LoggingCounter("az path.getProps");
    protected final LoggingCounter azPathList_Counter = new LoggingCounter("az path.listPaths");

    private int maxRetry = 3;

    protected static class AzPrefetchProposedPathItem extends PrefetchProposedPathItem {
    	protected AzureStorageNodeFsDataProvider delegate;
    	protected PrefetchNodeFsDataContext prefetchFsContext; // ?? converter
    	
    	protected PathItem azPathItem;
    	protected DataLakeDirectoryClient parentDirClient;
    	
		public AzPrefetchProposedPathItem(
				AzureStorageNodeFsDataProvider delegate,
				PrefetchNodeFsDataContext prefetchFsContext,
				NodeNamesPath path, 
				int type, long lastModifiedTime, long refreshTimeMillis,
				boolean forIncompleteData, 
				PathItem azPathItem,
    			DataLakeDirectoryClient parentDirClient) {
			super(path, type, lastModifiedTime, refreshTimeMillis, forIncompleteData);
			this.delegate = delegate;
			this.prefetchFsContext = prefetchFsContext;
			this.azPathItem = azPathItem;
			this.parentDirClient = parentDirClient;
		}

		@Override
		public void acceptToCompleteDataItem(PrefetchNodeDataContext prefetchContext) {
			delegate.acceptToCompleteDataItem(prefetchFsContext, this); // converter instead of prefetchContext?!  
		}
    };

    // --------------------------------------------------------------------------------------------
    
    public AzureStorageNodeFsDataProvider(
            String displayName, String displayBaseUrl, //
            DataLakeFileSystemClient fileSystemClient,
            DataLakeDirectoryClient baseDirClient,
            String fsSubDirPath,
            NodeNameEncoder nodeNameEncoder
            ) {
        this.displayName = displayName;
        this.displayBaseUrl = displayBaseUrl;
        this.fileSystemClient = fileSystemClient;
        this.baseDirClient = baseDirClient;
        this.fsSubDirPath = fsSubDirPath;
        this.nodeNameEncoder = nodeNameEncoder;
    }
    
	// --------------------------------------------------------------------------------------------
    
    // TODO use async api
    @Override
    public NodeFsData queryNodeFsData(NodeNamesPath path, PrefetchNodeFsDataContext prefetchCtx) {
        val pathSlash = path.toPathSlash();
        DataLakeDirectoryClient pathDirClient = dirClientForPath(pathSlash);

        PathProperties azPathProps;
        if (fsSubDirPath.isEmpty() && (pathSlash.equals("") || pathSlash.equals("/"))) {
            // querying base dir (maybe root filesystem?)
            azPathProps = doRetryableAzQueryPathProperties(pathSlash, pathDirClient);
        } else {
            azPathProps = doRetryableAzQueryPathProperties(pathSlash, pathDirClient);
        }

        val name = (path.pathElementCount() > 0)? path.lastName() : DefaultNodeNameEncoderOptions.EMPTY_NAME;
        long creationTime = AzDatalakeStoreUtils.toMillis(azPathProps.getCreationTime());
        long lastModifiedTime = AzDatalakeStoreUtils.toMillis(azPathProps.getLastModified());
        val extraFsAttrs = ImmutableMap.<String,Object>of(
                // TOADD?
                );
        
        NodeFsData res;
        boolean isDir = azPathProps.isDirectory();
        if (isDir) {
            val childNames = azQueryListPaths_recursiveProposePrefetchPathItems(
					path, prefetchCtx);

            res = new DirNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, childNames);
        } else {
            long fileLen = azPathProps.getFileSize();
            
            res = new FileNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, fileLen);
        }
        
        return res;
    }

    private PathProperties doRetryableAzQueryPathProperties(String path, DataLakeDirectoryClient pathDirClient) {
        PathProperties res;
        int retryCount = 0;
        for(;; retryCount++) {
            try {
                val now = System.currentTimeMillis();
                
                // *** az query getProperties ***
                res = pathDirClient.getProperties();
                
                val millis = System.currentTimeMillis() - now;
                azPathGetProps_Counter.incr(millis, prefix -> log.info(prefix + " " + path));
                break;
                
            } catch(Exception ex) {
                if (ex instanceof DataLakeStorageException) {
                    val ex2 = (DataLakeStorageException) ex;
                    val statusCode = ex2.getStatusCode();
                    val errorCode = ex2.getErrorCode();
                    if (statusCode == 404) {
                        // "Status code 404, (BlobNotFound)" => no data, return null
                        return null;
                    }
                    if (statusCode == 403) {
                        if (errorCode.equals("AuthorizationPermissionMismatch")) {
                            log.warn("Failed to az getProperties path:'" + pathDirClient.getDirectoryPath() + "' ... got " + statusCode + " " + errorCode);
                            throw ex;
                        } else if (errorCode.contains("Status code 403, (empty body)")) {
                            log.warn("Failed to az getProperties path:'" + pathDirClient.getDirectoryPath() + "' ... got " + statusCode + " " + errorCode + " => probably missing ACL, so not permission to read");
                            throw ex;
                        }
                    }
                }

                if (retryCount + 1 < maxRetry) {
                    log.error("Failed az query listPaths: " + displayName + " " + path + " .. retry [" + retryCount + "/" + maxRetry + "] ex:" + ex.getMessage());
                    AzDatalakeStoreUtils.sleepAfterNRetry(retryCount);
                } else {
                    log.error("Failed az query listPaths: " + displayName + " " + path + " .. rethrow " + ex.getMessage());
                    throw ex;
                }
            }
        }
        return res;
    }

    private DataLakeDirectoryClient dirClientForPath(String path) {
        if (path == null || path.isEmpty() || path.equals("/")) {
            // return fileSystemClient.getDirectoryClient("/");
            return baseDirClient.getSubdirectoryClient("/");
        }
        String relPath = (path.startsWith("/"))? path.substring(1) : path; 
        boolean debug = true;
        if (debug) {
            String fsPath = ((fsSubDirPath.isEmpty())? "" : fsSubDirPath + "/") + relPath;
            if (fsPath.isEmpty()) {
                fsPath = "/";
            }
            val res = fileSystemClient.getDirectoryClient(fsPath);
            return res;
        } else {
            val res = baseDirClient.getSubdirectoryClient(path);
            return res;
        }        
    }
    
    private NodeFsData filePathItemToIncompleteFsData(NodeNamesPath path, PathItem src, long refreshTimeMillis) {
        if (src.isDirectory()) throw new IllegalStateException();
        val name = path.lastNameOrEmpty();
        long lastModified = AzDatalakeStoreUtils.toMillis(src.getLastModified());
        val fileLength = src.getContentLength();
        // ignored fields available:
        //      String eTag;
        //      String group;
        //      String name;
        //      String owner;
        //      String permissions;
      
        long creationTime = 0L; // NOT known here!
        
        val extraFsAttrs = ImmutableMap.<String,Object>of(
                // TOADD?
                );
        return new FileNodeFsData(name, creationTime, lastModified, extraFsAttrs, fileLength);        
    }
    
    private NodeFsData dirPathItemToIncompleteFsData(NodeNamesPath path, PathItem src, long refreshTimeMillis,
    		TreeSet<NodeName> childNames) {
        if (src.isDirectory()) throw new IllegalStateException();
        val name = path.lastNameOrEmpty();
        long lastModified = AzDatalakeStoreUtils.toMillis(src.getLastModified());
        // ignored fields available:
        //      String eTag;
        //      String group;
        //      String name;
        //      String owner;
        //      String permissions;
      
        long creationTime = 0L; // NOT known here!
        
        val extraFsAttrs = ImmutableMap.<String,Object>of(
                // TOADD?
                );
        return new DirNodeFsData(name, creationTime, lastModified, extraFsAttrs, childNames);        
    }
    
    

    private List<PathItem> retryableAzQueryListPaths(String path, DataLakeDirectoryClient pathDirClient) {
        List<PathItem> res;
        int retryCount = 0;
        for(;; retryCount++) {
            try {
                val now = System.currentTimeMillis();
                
                res = new ArrayList<>();
                
                // *** az query listPaths (first page) ***
                Duration timeout = Duration.ofMinutes(5);
                PagedIterable<PathItem> pathItemsIterable = pathDirClient.listPaths(false, false, null, timeout);
                for(PathItem azChildPathItem : pathItemsIterable) { // *** az query list more page ***
                    res.add(azChildPathItem);
                }
    
                val millis = System.currentTimeMillis() - now;
                azPathList_Counter.incr(millis, prefix -> log.info(prefix + " " + path));
                
                break;
            } catch(RuntimeException ex) {
                if (retryCount + 1 < maxRetry) {
                    log.error("Failed az query listPaths: " + displayName + " " + path + " .. retry [" + retryCount + "/" + maxRetry + "] ex:" + ex.getMessage());
                    AzDatalakeStoreUtils.sleepAfterNRetry(retryCount);
                } else {
                    log.error("Failed az query listPaths: " + displayName + " " + path + " .. rethrow " + ex.getMessage());
                    throw ex;
                }
            }
        }
        return res;
    }


	private TreeSet<NodeName> azQueryListPaths_recursiveProposePrefetchPathItems(
			NodeNamesPath path, PrefetchNodeFsDataContext prefetchCtx
			) {
		
        val pathSlash = path.toPathSlash();
        DataLakeDirectoryClient pathDirClient = dirClientForPath(pathSlash);
		
		val refreshChildTimeMillis = System.currentTimeMillis();
		
		List<PathItem> azChildPathItems = retryableAzQueryListPaths(pathSlash, pathDirClient);
		
		boolean errorOnPrefetch = false;
		
		// increment recurseLevel by creating a new childPrefetchCtx
		PrefetchNodeFsDataContext childPrefetchCtx = (prefetchCtx.acceptRecurseProposePrefetchPathItems())? 
				prefetchCtx.createChildPrefetchFsContext() : null;

		val childNames = new TreeSet<NodeName>();
		for(PathItem azChildPathItem : azChildPathItems) {
			String childFilename = AzDatalakeStoreUtils.pathItemToChildName(azChildPathItem);
		    val childName = nodeNameEncoder.encode(childFilename);
		    childNames.add(childName);
		    
		    if (childPrefetchCtx != null && !errorOnPrefetch) {
		    	try {
		    		val childPath = path.toChild(childName);

		            doRecursiveProposePrefetchPathItem(childPrefetchCtx, 0,
		            		childPath, azChildPathItem, refreshChildTimeMillis,
		            		pathDirClient
		            		);

		        } catch(Exception ex) {
		            log.error("Failed doRecursiveProposePrefetchPathItem .. ignore, no rethrow " + ex.getMessage());
		            errorOnPrefetch  = true;
		        }
		    }
		}
		return childNames;
	}

	private void doRecursiveProposePrefetchPathItem(PrefetchNodeFsDataContext prefetchFsCtx,
			int recurseLevel,
			NodeNamesPath path,
			PathItem azPathItem,
			long refreshTimeMillis,
			DataLakeDirectoryClient parentDirClient
			) {
		if (! prefetchFsCtx.acceptPrefetchNodeDatas()) {
			return; // method has no other side-effect, simply skip
		}
		// TOADD... may also send childData ... even if incomplete, because missing creationTime
		// only for files... not for sub dir
		if (! azPathItem.isDirectory()) {
		    val childFsData = filePathItemToIncompleteFsData(path, azPathItem, refreshTimeMillis);
		    prefetchFsCtx.onPrefetchNodeFsData(path, childFsData, refreshTimeMillis, true); // isIncomplete
		} else { // isDirectory
		    // TOADD... transfer partial az PathItem, so as to avoid next az getProperties() !!
		    // => incomplete data to be loaded next with az listPath() only
			long lastModifiedTime = AzDatalakeStoreUtils.toMillis(azPathItem.getLastModified());
			// val childFsData = dirPathItemToIncompleteFsData(childPath, azChildPathItem, refreshChildTimeMillis);

			val proposePathItem = new AzPrefetchProposedPathItem(this, prefetchFsCtx,
					path, NodeData.TYPE_DIR, lastModifiedTime, refreshTimeMillis,
					true, // forIncompleteData, 
					azPathItem,
	    			parentDirClient);

			prefetchFsCtx.onProposePrefetchPathItem(proposePathItem);
			// => ctx will respond by acceptToCompleteDataItem() or ignoreTocomplete()
			// ... therefore ***recurse***
		}
	}

    protected void acceptToCompleteDataItem(PrefetchNodeFsDataContext prefetchContext,
			AzPrefetchProposedPathItem azProposedPathItem) {
    	PathItem azPathItem = azProposedPathItem.azPathItem;
		val path = azProposedPathItem.getPath();
		val refreshTimeMillis = azProposedPathItem.getRefreshTimeMillis();
		
    	if (! azPathItem.isDirectory()) {
    		// isFile ... (should not occur here)
    		val fsData = filePathItemToIncompleteFsData(path, azPathItem, refreshTimeMillis);

    		prefetchContext.onPrefetchNodeFsData(path, fsData, refreshTimeMillis, 
    				true); // incomplete: do not contains creationTime

    	} else { // isDirectory 
	    	// *** do az query path listPaths() ... and recursive propose child ***
	    	TreeSet<NodeName> childNames = azQueryListPaths_recursiveProposePrefetchPathItems(path, prefetchContext);
	    	
	    	// build DirNodeFsData, propose  (child-child already recursively proposed above)
	    	val fsData = dirPathItemToIncompleteFsData(path, azPathItem, refreshTimeMillis, childNames); 

	    	prefetchContext.onPrefetchNodeFsData(path, fsData, refreshTimeMillis, 
	    			true); // incomplete: do not contains creationTime
    	}
	}

}
