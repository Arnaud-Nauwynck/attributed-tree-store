package fr.an.attrtreestore.s3;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeNamesPath;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * optionnal helper used in S3NodeFsDataProvider
 * to deduce S3 metadata for Dirs from bucket + key "yyyy-mm-dd/.."
 * as there is NO S3 'object' for Dirs, only prefix strings... 
 * 
 */
public abstract class S3DirMetadataCompleter {

    @NoArgsConstructor @AllArgsConstructor
    public static class S3CompletedMetadata {
        public long creationTime;
        public long lastModifiedTime;
        public ImmutableMap<String,Object> extraFsAttrs;
    }
    
    public abstract S3CompletedMetadata tryCompleteMetadataForDir(String bucketName, NodeNamesPath path);
    
    
    // --------------------------------------------------------------------------------------------
    
    @AllArgsConstructor
    public static class DefaultS3DirMetadataCompleter extends S3DirMetadataCompleter {
        
        public final S3CompletedMetadata completedMetadata;
        
        @Override
        public S3CompletedMetadata tryCompleteMetadataForDir(String bucketName, NodeNamesPath path) {
            return new S3CompletedMetadata(completedMetadata.creationTime, completedMetadata.lastModifiedTime, completedMetadata.extraFsAttrs);
        }
    }

}
