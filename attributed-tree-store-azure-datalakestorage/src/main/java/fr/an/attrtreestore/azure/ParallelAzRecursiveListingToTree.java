package fr.an.attrtreestore.azure;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.models.PathItem;

import fr.an.attrtreestore.api.IWriteTreeData;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import lombok.val;

public class ParallelAzRecursiveListingToTree extends AzRecursiveListingToTree {

	protected final ExecutorService executorService;
	
	// ------------------------------------------------------------------------
	
	public ParallelAzRecursiveListingToTree(IWriteTreeData destTree, NodeNameEncoder nameEncoder,
			ExecutorService executorService) {
		super(destTree, nameEncoder);
		this.executorService = executorService;
	}

	// ------------------------------------------------------------------------
	
	@Override
	protected void putChildPathItemList_recurseOnMissingDirChildItems(DataLakeDirectoryClient parentDirClient,
			NodeNamesPath parentDestPath, 
			List<PathItem> childPathItemsToRecurse) {
		for(val childPathItem: childPathItemsToRecurse) {
			val childFilename = AzDatalakeStoreUtils.pathItemToChildName(childPathItem);
			val childName = nameEncoder.encode(childFilename);
			val childPath = parentDestPath.toChild(childName);
			
			putPathItem_recurseOnMissingDirChildItems(childPathItem, childPath, parentDirClient);
		}
	}

	
	
}
