package fr.an.attrtreestore.s3;

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.cachedfsview.converter.NodeFsDataToNodeDataConverter;
import fr.an.attrtreestore.cachedfsview.converter.NodeFsDataToNodeDataConverter.DefaultNodeFsDataToNodeDataConverter;
import fr.an.attrtreestore.storage.impl.PersistedTreeData;
import fr.an.attrtreestore.util.AttrTreeStoreUtils;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3DirListing {

    private final S3NodeFsDataProvider s3NodeFsDataProvider;
    
    protected final IWriteTreeData destTree;

    protected final NodeNameEncoder nodeNameEncoder;
    
    @Getter
    private NodeFsDataToNodeDataConverter fsDataToNodeDataConverter = DefaultNodeFsDataToNodeDataConverter.INSTANCE;
    
    
    @Getter
    protected LoggingCounter counter_s3DirListingPageResp = new LoggingCounter("s3 listObjectsV2.page",
            new LoggingCounterParams(100_000, 3*60_000)); // 100k or 3mn

    @Getter
    protected LoggingCounter counter_s3DirListing_PutFile = new LoggingCounter("s3 listObjectsV2-handle put file",
            new LoggingCounterParams(100_000, 3*60_000)); // 100k or 3mn

    @Getter
    protected LoggingCounter counter_s3DirListing_PutDir = new LoggingCounter("s3 listObjectsV2-handle put dir",
            new LoggingCounterParams(100_000, 3*60_000)); // 100k or 3mn

    @Getter
    protected LoggingCounter counter_s3DirListing_AlreadyExistFile = new LoggingCounter("s3 listObjectsV2-handle already exist file",
            new LoggingCounterParams(100_000, 3*60_000)); // 100k or 3mn

    @Getter
    protected LoggingCounter counter_s3DirListing_AlreadyExistDir = new LoggingCounter("s3 listObjectsV2-handle already exist dir",
            new LoggingCounterParams(100_000, 3*60_000)); // 100k or 3mn

    
    private AtomicBoolean requestInterrupt = new AtomicBoolean();
    
    // --------------------------------------------------------------------------------------------

    public S3DirListing(S3NodeFsDataProvider s3NodeFsDataProvider,
            IWriteTreeData destTree,
            NodeNameEncoder nodeNameEncoder) {
        this.s3NodeFsDataProvider = s3NodeFsDataProvider;
        this.destTree = destTree;
        this.nodeNameEncoder = nodeNameEncoder;
    }
    
    // --------------------------------------------------------------------------------------------
    
    public void interrupt() {
        requestInterrupt.set(true);
    }
    
    public void executeListing(NodeNamesPath scanBasePath) {
        val startTime = System.currentTimeMillis();
        val bucketName = s3NodeFsDataProvider.getS3BucketName();
        log.info("start scan of S3 bucket " + bucketName 
                + ((scanBasePath.pathElementCount() != 0)? " basePath: " + scanBasePath : "")
                );
        try {
            requestInterrupt.set(false);
            
            doScan(scanBasePath);
            
            long millis = System.currentTimeMillis() - startTime;
            log.info("finished scan " 
                    + ((requestInterrupt.get())? "(reason:interupted)" : "")
                    + " of S3 bucket " + bucketName + " basePath: " + scanBasePath
                    + " .. took " + AttrTreeStoreUtils.millisToText(millis)
                    );
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            log.error("Failed scan of S3 bucket " + bucketName + " basePath:" + scanBasePath 
                    + " .. after " + AttrTreeStoreUtils.millisToText(millis), ex);
        }
    }

    private void doScan(NodeNamesPath scanBasePath) {
        val bucketName = s3NodeFsDataProvider.getS3BucketName();
        val s3Client = s3NodeFsDataProvider.getS3Client();
        
        String scanBasePathText = scanBasePath.toPathSlash();

        int totalS3ObjectSummaries = 0;
        int totalS3CommonPrefixes = 0;
        long totalS3FileLen = 0;

        long prevTime = System.currentTimeMillis();
        try (val writer = new SortedScanResultWriter(prevTime)) {
            ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                // .withStartAfter(startAfter) // ?
                ;
            if (! scanBasePathText.isEmpty()) {
                req.withPrefix(scanBasePathText);
            } 
            // req.setSdkRequestTimeout(sdkRequestTimeout);
            
            ListObjectsV2Result result;
            do {
                // **** The Biggy ***  repeated for pages(max:1000) many times(x3000 ?), may take 20ms-1sec each, average 50ms
                // can take 13mn total time
                result = s3Client.listObjectsV2(req);
       
                val s3ObjectSummaries = result.getObjectSummaries();
                val s3CommonPrefixes = result.getCommonPrefixes(); // no delimiter, so expected empty
    
                totalS3ObjectSummaries += s3ObjectSummaries.size(); // expected max +1000
                totalS3CommonPrefixes += s3CommonPrefixes.size(); // expected 0
                for(val obj: s3ObjectSummaries) {
                    totalS3FileLen += obj.getSize();
                }
                        
                val timeNow = System.currentTimeMillis();
                val millis = timeNow - prevTime;
                prevTime = timeNow;
                String currKey;
                if (! s3ObjectSummaries.isEmpty()) {
                    val first = s3ObjectSummaries.get(0);
                    currKey = first.getKey();
                } else if (! s3CommonPrefixes.isEmpty()) {
                    currKey = s3CommonPrefixes.get(0);
                } else {
                    currKey = "?";
                }
                val fCurrKey = currKey;
                val fCurrCountObjectsSummaries = totalS3ObjectSummaries;
                val fTotalS3FileLen = totalS3FileLen;
                final long MEGA = 1024*1024; 
                counter_s3DirListingPageResp.incr(millis, logPrefix -> log.info(logPrefix + " " + bucketName + " " + fCurrKey
                        + ", curr count:" + fCurrCountObjectsSummaries + ", currTotalFileLen: " + (fTotalS3FileLen / MEGA) + " MB"));
                
                writer.handleResult(s3ObjectSummaries);
                
                if (requestInterrupt.get()) {
                    break;
                }
                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
        }
        
        // TODO wait writer flush
        
        log.info("scan " 
                + " S3 bucket " + bucketName + " basePath: " + scanBasePath
                + " => found: " + totalS3ObjectSummaries + " objects "
                + " total File Len:" + totalS3FileLen
                + ((totalS3CommonPrefixes != 0)? " + " + totalS3CommonPrefixes + " commonPrefixes" : "")
                );
    }

    @RequiredArgsConstructor
    protected static class DirBuilder {
        final NodeNamesPath dirPath; 
        // Map<NodeName,NodeData> currDirChildMap = new L
        final TreeSet<NodeName> childNames = new TreeSet<>();
    }
    
    protected class SortedScanResultWriter implements Closeable {
        private final long refreshTimeMillis;
        
        private String currDirPathText;
        private NodeNamesPath currDirPath;
        private final List<DirBuilder> currDirStack = new ArrayList<>();
        
        public SortedScanResultWriter(long refreshTimeMillis) {
            this.refreshTimeMillis = refreshTimeMillis;
            this.currDirPathText = "";
            this.currDirPath = NodeNamesPath.ROOT;
            this.currDirStack.add(new DirBuilder(NodeNamesPath.ROOT));
        }

        @Override
        public void close() {
            popDirs_putToTree(currDirPath, NodeNamesPath.ROOT);
            popDir_putToTree(); // root dir
            
            if (destTree instanceof PersistedTreeData) {
                val tree2 = (PersistedTreeData) destTree;
                // tree2 flushStopWrite
            }
        }

        public void handleResult(List<S3ObjectSummary> objectSummaries) {
            for(val objectSummary: objectSummaries) {
                doHandleResult(objectSummary);
            }
        }

        private void doHandleResult(S3ObjectSummary objectSummary) {
            val bucketName = objectSummary.getBucketName();
            String key = objectSummary.getKey();
            int sep = key.lastIndexOf("/");
            val lastNameText = (sep != -1)? key.substring(sep + 1) : key;
            val childName = nodeNameEncoder.encode(lastNameText);
            val parentDirPathText = (sep != -1)? key.substring(0, sep - 1) : key;
            NodeNamesPath path = currDirPath;
            if (! parentDirPathText.equals(currDirPathText)) {
                path = nodeNameEncoder.encodePath(parentDirPathText);
                // example prevPath: "a/b/c1/d1"
                //             path: "a/b/c2/d2"
                // =>   common path: "a/b"
                //   pop path to create dirs: "a/b/c1/d1", a/b/c1"
                //   tmp dirs to push: "a/b/c2", "a/b/c2/d2"
                val commonPath = NodeNamesPath.commonPathOf(currDirPath, path);
                if (! currDirPathText.isEmpty()) {
                    popDirs_putToTree(currDirPath, commonPath);
                }
                pushDirs(commonPath, path);
                this.currDirPathText = parentDirPathText;
            }
            
            addChildToCurrDir(childName);
            
            // update/create file data
            long startTimePut = System.currentTimeMillis();
            val filePath = path.toChild(childName);
            val foundFileData = null; // destTree.get(filePath); TODO
            if (foundFileData == null) {
                val fileFsData = s3NodeFsDataProvider.s3ObjectSummaryToFileNodeFsData(childName, objectSummary);
                val fileData = fsDataToNodeDataConverter.nodeFsDataToNodeData(filePath, fileFsData, refreshTimeMillis, false);
                destTree.put(filePath, fileData);
                
                long millis = System.currentTimeMillis() - startTimePut;
                counter_s3DirListing_PutFile.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + filePath.toPathSlash() + ")"));
            } else { 
                // already exist .. assume no update
                long millis = System.currentTimeMillis() - startTimePut;
                counter_s3DirListing_AlreadyExistFile.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + filePath.toPathSlash() + ")"));
            }
        }
        

        public void pushDir(NodeNamesPath path) {
            addChildToCurrDir(path.lastName());
            this.currDirPath = path;
            this.currDirStack.add(new DirBuilder(path));
        }
        
        public void addChildToCurrDir(NodeName childName) {
            this.currDirStack.get(this.currDirStack.size()-1).childNames.add(childName);
        }
        
        public DirBuilder popDir() {
            return this.currDirStack.remove(this.currDirStack.size()-1);
        }
        
        protected void popDirs_putToTree(NodeNamesPath prevDirPath, NodeNamesPath toParentPath) {
            val toPathElementCount = toParentPath.pathElementCount();
            for(int i = prevDirPath.pathElementCount()-1; i >= toPathElementCount; i--) {
                popDir_putToTree();
            }
        }
        
        protected void popDir_putToTree() {
            val bucketName = s3NodeFsDataProvider.getS3BucketName();
            val dirBuilder = popDir();
            long startTimePut = System.currentTimeMillis();
            val dirPath = dirBuilder.dirPath;
            val childNames = dirBuilder.childNames;
            val foundDirData = null; // destTree.get(dirPath); TODO
            if (foundDirData == null) {
                NodeFsData dirFsData = s3NodeFsDataProvider.s3ChildListToDirNodeFsData(dirPath, childNames);
                val dirData = fsDataToNodeDataConverter.nodeFsDataToNodeData(dirPath, dirFsData, refreshTimeMillis, false);
                
                destTree.put(dirPath, dirData);

                long millis = System.currentTimeMillis() - startTimePut;
                counter_s3DirListing_PutDir.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", "+ dirPath.toPathSlash() + ")"));
            } else { 
                // already exist .. assume no update
                long millis = System.currentTimeMillis() - startTimePut;
                counter_s3DirListing_AlreadyExistDir.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + dirPath.toPathSlash() + ")"));
            }
        }
        
        protected void pushDirs(NodeNamesPath fromParentPath, NodeNamesPath toPath) {
            val toPathElementCount = toPath.pathElementCount();
            for(int i = fromParentPath.pathElementCount() + 1; i <= toPathElementCount; i++) {
                val path = toPath.subPath(i);
                pushDir(path);
            }
        }
    }
    
}
