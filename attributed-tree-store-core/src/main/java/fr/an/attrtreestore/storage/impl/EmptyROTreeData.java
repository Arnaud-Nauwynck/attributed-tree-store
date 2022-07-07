package fr.an.attrtreestore.storage.impl;

import fr.an.attrtreestore.api.NodeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.ROCached_TreeData;

public class EmptyROTreeData extends ROCached_TreeData {
    
    public EmptyROTreeData() {
    }

    @Override
    protected void init(IndexedBlobStorageInitMode initMode, long initPreloadSize) {
        // do nothing
    }

    @Override
    public NodeData get(NodeNamesPath path) {
        return null;
    }
    
}
