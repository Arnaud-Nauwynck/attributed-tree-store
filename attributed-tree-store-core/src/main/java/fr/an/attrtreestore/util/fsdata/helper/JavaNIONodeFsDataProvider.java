package fr.an.attrtreestore.util.fsdata.helper;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.TreeSet;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.cachedfsview.PrefetchOtherNodeFsDataCallback;
import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import lombok.Getter;
import lombok.val;

public class JavaNIONodeFsDataProvider extends NodeFsDataProvider {
	
	private final NodeNameEncoder nodeNameEncoder;

	@Getter
	private final java.nio.file.Path baseDirPath;
	
	private final String baseDirPathText;
	
	// ------------------------------------------------------------------------
	
	public JavaNIONodeFsDataProvider(NodeNameEncoder nodeNameEncoder, Path baseDirPath) {
		this.nodeNameEncoder = nodeNameEncoder;
		this.baseDirPath = baseDirPath;
		this.baseDirPathText = baseDirPath.toString();
	}

	// ------------------------------------------------------------------------

	@Override
	public NodeFsData queryNodeFsData(NodeNamesPath subpath, PrefetchOtherNodeFsDataCallback optCallback) {
		String[] subpathTexts = subpath.toTexts();
		// Paths.get(baseDirPath, subpathTexts);
		Path nioPath = FileSystems.getDefault().getPath(baseDirPathText, subpathTexts);
		BasicFileAttributeView nioAttrView = Files.getFileAttributeView(nioPath, BasicFileAttributeView.class);
		BasicFileAttributes nioAttr;
		try {
			nioAttr = nioAttrView.readAttributes();
		} catch (IOException ex) {
			if (! Files.exists(nioPath)) { // retry
				return null;
			}
			throw new RuntimeException("Failed to read file attr", ex);
		}
		
		NodeName name = subpath.lastName();
		
		long creationTime = nioAttr.creationTime().toMillis();
		long lastModifiedTime = nioAttr.lastModifiedTime().toMillis();
		
		NodeFsData res;
		if (nioAttr.isDirectory()) {
			val extraFsAttrs = ImmutableMap.<String,Object> of(
					// TOADD
					);
			val childNames = new TreeSet<NodeName>();
			Stream<Path> childLs = null;
			try {
				childLs = Files.list(nioPath);
				childLs.forEach(child -> {
					val childName = nodeNameEncoder.encode(child.getFileName().toString());
					childNames.add(childName);
				});
			} catch (IOException ex) {
				throw new RuntimeException("Failed to list " + nioPath, ex);
			} finally {
				if (childLs != null) {
					childLs.close();
				}
			}
			res = new DirNodeFsData(name, creationTime, lastModifiedTime, // 
					extraFsAttrs, childNames);
		} else if (nioAttr.isRegularFile()) {
			val extraFsAttrs = ImmutableMap.<String,Object> of(
					// TOADD
					);
			long fileLength = nioAttr.size(); 
			res = new FileNodeFsData(name, creationTime, lastModifiedTime, // 
					extraFsAttrs, fileLength);
		} else {
			res = null; // unsupported yet.. ignore, return null
		}
		return res;
	}

}
