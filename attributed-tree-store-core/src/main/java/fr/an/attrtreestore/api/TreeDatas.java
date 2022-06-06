package fr.an.attrtreestore.api;

import fr.an.attrtreestore.spi.BlobStorage;
import fr.an.attrtreestore.storage.AttrDataEncoderHelper;
import fr.an.attrtreestore.storage.impl.PersistedTreeData;

public class TreeDatas {

	public static PersistedTreeData createPersistedTree(BlobStorage blobStorage, String baseDirname,
			AttrDataEncoderHelper attrDataEncoderHelper) {
		return new PersistedTreeData(blobStorage, baseDirname, attrDataEncoderHelper);
	}

}
