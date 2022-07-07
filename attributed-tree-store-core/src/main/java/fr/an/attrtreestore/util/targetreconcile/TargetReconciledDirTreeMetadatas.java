package fr.an.attrtreestore.util.targetreconcile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.path4j.NodeName;
import org.path4j.NodeNamesPath;
import org.path4j.impl.StringNodeName;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

public class TargetReconciledDirTreeMetadatas {

    @Getter
    private final TargetDirMetadataEntry rootDir = new TargetDirMetadataEntry(NodeName.EMPTY);
    
    private static Map<Integer,NodeName> packageIdxNodeNames = new HashMap<>();
    private static synchronized NodeName packageIdxNodeName(int idx) {
        return packageIdxNodeNames.computeIfAbsent(idx, k -> new StringNodeName("package-" + k));
    }
    
    public TargetDirMetadataEntry getOrCreateDir(NodeNamesPath path) {
        if (path == null || path.size() == 0) {
            return rootDir;
        }
        return rootDir.getOrCreateChildDir(path,  0);
    }

    // --------------------------------------------------------------------------------------------
    
    @RequiredArgsConstructor
    public static abstract class TargetMetadataEntry {
        
        public final NodeName name;

        @Override
        public String toString() {
            return "{" + name + "}";
        }
        
    }

    /**
     * target dir
     */
    public static class TargetDirMetadataEntry extends TargetMetadataEntry {

        @Getter
        private final LinkedHashMap<NodeName,TargetMetadataEntry> child = new LinkedHashMap<>();

        private int currPackageChildFileIdx = 0;
        private TargetZipPackagedFileMetadataEntry currPackageChildFile; 
        
        public TargetDirMetadataEntry(NodeName name) {
            super(name);
        }
        
        public void addChild(TargetMetadataEntry p) {
            child.put(p.name, p);
        }
        
        public TargetDirMetadataEntry getOrCreateChildDir(NodeName name) {
            TargetDirMetadataEntry res;
            TargetMetadataEntry found = child.get(name);
            if (found == null) {
                res = new TargetDirMetadataEntry(name);
                child.put(name, res);
            } else {
               if (!(found instanceof TargetDirMetadataEntry)) {
                   throw new IllegalArgumentException("child '" + name + "' already exists, but is not a dir");
               }
               res = (TargetDirMetadataEntry) found;
            }
            return res;
        }

        public TargetDirMetadataEntry getOrCreateChildDir(NodeNamesPath subPath, int fromIdx) {
            int len = subPath.size();
            TargetDirMetadataEntry curr = this;
            for(int i = fromIdx; i < len; i++) {
                val childName = subPath.get(i);
                curr = curr.getOrCreateChildDir(childName);
            }
            return curr;
        }

        public TargetZipPackagedFileMetadataEntry getOrCreatePackageFile() {
            TargetZipPackagedFileMetadataEntry res = currPackageChildFile; 
            if (res == null) {
                res = rollNewPackageFile();
            }
            return res;
        }
        
        public TargetZipPackagedFileMetadataEntry rollNewPackageFile() {
            val idx = ++currPackageChildFileIdx;
            val name = packageIdxNodeName(idx);
            this.currPackageChildFile = new TargetZipPackagedFileMetadataEntry(name);
            this.child.put(name, currPackageChildFile);
            return currPackageChildFile; 
        }
        
        
        @Override
        public String toString() {
            return "{Dir: " + name + "}";
        }

    }
    
    /**
     * target files to be copied
     */
    @Getter
    public static class TargetCopyFileMetadataEntry extends TargetMetadataEntry {
        
        public final NodeNamesPath srcPath;
        public final long fileLength;
        // public final String fileMD5;
        
        public TargetCopyFileMetadataEntry(NodeName name, NodeNamesPath srcPath, long fileLength) {
            super(name);
            this.srcPath = srcPath;
            this.fileLength = fileLength;
        }
        
    }
    
    /**
     * target zip entry into ZipPackagedFile
     */
    @RequiredArgsConstructor
    public static class TargetZipEntry {
        public final NodeNamesPath zipEntryPath;
        public final NodeNamesPath srcPath;
        public final long fileLength;
        // public final String fileMD5;
        
        @Override
        public String toString() {
            return "{TargetZipEntry: " + zipEntryPath + ", len:" + fileLength + "}";
        }
    }

    /**
     * target files to be packaged as a single (zip) file
     */
    public static class TargetZipPackagedFileMetadataEntry extends TargetMetadataEntry {
        
        @Getter
        private final LinkedHashMap<NodeNamesPath,TargetZipEntry> zipEntries = new LinkedHashMap<>();
        
        @Getter
        private long totalEntriesLength;
        
        public TargetZipPackagedFileMetadataEntry(NodeName name) {
            super(name);
        }

        public void addEntry(NodeNamesPath zipEntryPath, NodeNamesPath srcPath, long fileLength) {
            val zipEntry = new TargetZipEntry(zipEntryPath, srcPath, fileLength);
            zipEntries.put(zipEntryPath, zipEntry);
            this.totalEntriesLength += fileLength;
        }

        @Override
        public String toString() {
            // display first + last path (if different of first) 
            val entriesCount = zipEntries.size();
            val tmpLs = new ArrayList<>(zipEntries.values());
            val first = (entriesCount != 0)? tmpLs.get(0) : null;
            val optLast = (entriesCount > 1)? tmpLs.get(entriesCount  - 1) : null;
            return "{TargetZipPackagedFile " + name //
                    + ", zipEntries.count=" + entriesCount // 
                    + ", totalEntriesLength=" + totalEntriesLength
                    + ((first != null)? ", firstEntryPath:" + first.zipEntryPath : "")
                    + ((optLast != null)? ", lastEntryPath:" + optLast.zipEntryPath : "")
                    + "}";
        }
        
    }

}
