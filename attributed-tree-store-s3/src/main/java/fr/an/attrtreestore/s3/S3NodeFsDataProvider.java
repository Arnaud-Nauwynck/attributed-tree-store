package fr.an.attrtreestore.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.List;
import java.util.TreeSet;

import org.path4j.NodeName;
import org.path4j.NodeNameEncoder;
import org.path4j.NodeNamesPath;
import org.simplestorage4j.s3.S3Client;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.readprefetch.PrefetchNodeDataContext;
import fr.an.attrtreestore.api.readprefetch.PrefetchProposedPathItem;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.cachedfsview.PrefetchNodeFsDataContext;
import fr.an.attrtreestore.util.AttrTreeStoreUtils;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.Getter;
import lombok.val;

/**
 * 
 */
public class S3NodeFsDataProvider extends NodeFsDataProvider {

    @Getter
    private final String displayName;
    @Getter
    private final String displayBaseUrl;
    
    @Getter
    private final S3Client s3Client;

    @Getter
    private final String s3BucketName;

    @Getter
    private final NodeNameEncoder nodeNameEncoder;

    @Getter
    private final S3DirMetadataCompleter s3DirMetadataCompleter;
    
    // --------------------------------------------------------------------------------------------
    
    public S3NodeFsDataProvider(
            String displayName, String displayBaseUrl, //
            S3Client s3Client,
            String s3BucketName,
            S3DirMetadataCompleter s3DirMetadataCompleter,
            NodeNameEncoder nodeNameEncoder
            ) {
        this.displayName = displayName;
        this.displayBaseUrl = displayBaseUrl;
        this.s3Client = s3Client;
        this.s3BucketName = s3BucketName;
        this.s3DirMetadataCompleter = s3DirMetadataCompleter;
        this.nodeNameEncoder = nodeNameEncoder;
    }
    
	// --------------------------------------------------------------------------------------------
    
    @Override
    public NodeFsData queryNodeFsData(NodeNamesPath path, PrefetchNodeFsDataContext prefetchCtx) {
        NodeFsData res;
        val pathText = path.toPathSlash();
        val name = path.lastOrEmpty();
        String s3Prefix = pathText; // may add convert? 
        if (! s3Prefix.endsWith("/")) {
            s3Prefix += "/";
        }
        long refreshTimeMillis = System.currentTimeMillis();
        
        val s3ChildList = s3Client.listObjectsV2_fetchAll(s3BucketName, s3Prefix);
        
        List<String> commonPrefixes = s3ChildList.commonPrefixes;
        List<S3ObjectSummary> objectSummmaries = s3ChildList.objectSummmaries;
        
        // TOCHECK ... if only 1 objectSummmaries with key=path => "File"
        
        if (! commonPrefixes.isEmpty() || ! objectSummmaries.isEmpty()) {
            // considered as "Dir" object (but no metadata in S3 for prefix)
            res = s3ChildListToDirNodeFsData(s3Prefix, path, commonPrefixes, objectSummmaries);
            
            if (! objectSummmaries.isEmpty()) {
                doProposePrefetchObjectSummaries(prefetchCtx, 1, path, objectSummmaries, refreshTimeMillis);
            }
        } else {
            // get object metadata if exists
            val objMetadata = s3Client.getObjectMetadata(s3BucketName, pathText);
            if (objMetadata != null) {
                // "File" object                
                res = s3ObjectMetadataToFileNodeFsData(name, objMetadata);
            } else {
                return null;
            }
        }
        return res;
    }

    private NodeFsData s3ChildListToDirNodeFsData(String prefix, NodeNamesPath path, 
            List<String> commonPrefixes, List<S3ObjectSummary> objectSummmaries) {
        val childNames = new TreeSet<NodeName>();
        if (! commonPrefixes.isEmpty()) {
            childNames.addAll(AttrTreeStoreUtils.map(commonPrefixes, x -> commonPrefixToLastName(x)));
        }
        if (! objectSummmaries.isEmpty()) {
            childNames.addAll(AttrTreeStoreUtils.map(objectSummmaries, x -> keyToLastName(x.getKey())));
        }
        val res = s3ChildListToDirNodeFsData(path, childNames);
        return res;
    }
    
    public NodeFsData s3ChildListToDirNodeFsData(NodeNamesPath path, TreeSet<NodeName> childNames) { 
        long creationTime = 0; // no S3 metadata! ... may deduce from bucket + key "yyyy-mm-dd/.."
        long lastModifiedTime = 0;
        ImmutableMap<String,Object> extraFsAttrs = ImmutableMap.<String,Object>of();
        if (s3DirMetadataCompleter != null) {
            val completeMetadata = s3DirMetadataCompleter.tryCompleteMetadataForDir(s3BucketName, path);
            if (completeMetadata != null) {
                creationTime = completeMetadata.creationTime;
                lastModifiedTime = completeMetadata.lastModifiedTime;
                extraFsAttrs = completeMetadata.extraFsAttrs;
            }
        }

        val name = path.lastOrEmpty();
        val res = new DirNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, childNames);
        return res;
    }

    private NodeName commonPrefixToLastName(String commonPrefix) {
        int lastIdx = commonPrefix.length() ;
        if (commonPrefix.endsWith("/")) {
            lastIdx--;
        }
        int fromIdx = commonPrefix.lastIndexOf("/", lastIdx-1);
        String name = commonPrefix.substring(fromIdx + 1, lastIdx);
        return nodeNameEncoder.encode(name);
    }

    public NodeName keyToLastName(String key) {
        int sep = key.lastIndexOf("/");
        val lastName = (sep != -1)? key.substring(sep + 1) : key;
        return nodeNameEncoder.encode(lastName);
    }
    
    protected FileNodeFsData s3ObjectMetadataToFileNodeFsData(NodeName name, ObjectMetadata s3ObjectMetadata) {
        Date lastModDate = s3ObjectMetadata.getLastModified();
        long lastModifiedTime = (lastModDate != null)? lastModDate.getTime() : 0;
        long creationTime = lastModifiedTime; // no S3 metadata for create? or need versions?
        long fileLen = s3ObjectMetadata.getContentLength();
        // String md5 = objMetadata.getContentMD5();
         
        val extraFsAttrs = ImmutableMap.<String,Object>of(
                // TOADD md5 ..
                );

        return new FileNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, fileLen);
    }

    public FileNodeFsData s3ObjectSummaryToFileNodeFsData(NodeName name, S3ObjectSummary s3ObjectSummary) {
        Date lastModDate = s3ObjectSummary.getLastModified();
        long lastModifiedTime = lastModDate.getTime();
        long creationTime = lastModifiedTime; // no S3 metadata for create? or need versions?
        long fileLen = s3ObjectSummary.getSize();
        // String md5 = s3ObjectSummary.getETag();
         
        val extraFsAttrs = ImmutableMap.<String,Object>of(
                // TOADD md5 ..
                );

        return new FileNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, fileLen);
    }

    private void doProposePrefetchObjectSummaries(PrefetchNodeFsDataContext prefetchFsCtx,
            int recurseLevel,
            NodeNamesPath parentPath,
            List<S3ObjectSummary> s3ObjectSummaries,
            long refreshTimeMillis
            ) {
        if (! prefetchFsCtx.acceptPrefetchNodeDatas()) {
            return; // method has no other side-effect, simply skip
        }
        for(val s3ObjectSummary: s3ObjectSummaries) {
            val lastModifiedTime = s3ObjectSummary.getLastModified().getTime();
            val childName = keyToLastName(s3ObjectSummary.getKey());
            val path = parentPath.toChild(childName);
            val proposePathItem = new S3PrefetchProposedPathItem(this, prefetchFsCtx,
                    path, lastModifiedTime, refreshTimeMillis, s3ObjectSummary);

            prefetchFsCtx.onProposePrefetchPathItem(proposePathItem);
            // => ctx will respond by acceptToCompleteDataItem() or ignoreTocomplete()
            // ... therefore ***recurse***
        }
    }

    protected static class S3PrefetchProposedPathItem extends PrefetchProposedPathItem {
        protected S3NodeFsDataProvider delegate;
        protected PrefetchNodeFsDataContext prefetchFsContext;
        
        protected S3ObjectSummary s3ObjectSummary;
        
        public S3PrefetchProposedPathItem(
                S3NodeFsDataProvider delegate,
                PrefetchNodeFsDataContext prefetchFsContext,
                NodeNamesPath path, 
                long lastModifiedTime, long refreshTimeMillis,
                S3ObjectSummary s3ObjectSummary) {
            super(path, NodeData.TYPE_FILE, lastModifiedTime, refreshTimeMillis, false);
            this.delegate = delegate;
            this.prefetchFsContext = prefetchFsContext;
            this.s3ObjectSummary = s3ObjectSummary;
        }

        @Override
        public void acceptToCompleteDataItem(PrefetchNodeDataContext prefetchContext) {
            delegate.acceptToCompleteDataItem(prefetchFsContext, this);  
        }
    }

    protected void acceptToCompleteDataItem(PrefetchNodeFsDataContext prefetchContext,
            S3PrefetchProposedPathItem s3ProposedPathItem) {
        val s3ObjectSummary = s3ProposedPathItem.s3ObjectSummary;
        val path = s3ProposedPathItem.getPath();
        val name = path.lastOrEmpty();
        val fsData = s3ObjectSummaryToFileNodeFsData(name, s3ObjectSummary);

        val refreshTimeMillis = s3ProposedPathItem.getRefreshTimeMillis();
        prefetchContext.onPrefetchNodeFsData(path, fsData, refreshTimeMillis, false);
    }
    
}
