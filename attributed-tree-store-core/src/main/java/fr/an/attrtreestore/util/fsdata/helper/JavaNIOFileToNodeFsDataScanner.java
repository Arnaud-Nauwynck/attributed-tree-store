package fr.an.attrtreestore.util.fsdata.helper;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.path4j.NodeName;
import org.path4j.NodeNameEncoder;
import org.path4j.NodeNamesPath;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.util.fsdata.NodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.DirNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsData.FileNodeFsData;
import fr.an.attrtreestore.util.fsdata.NodeFsDataVisitor;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JavaNIOFileToNodeFsDataScanner {

	public static void scan(Path rootScanDir,
			NodeNameEncoder nodeNameEncoder,
			NodeFsDataVisitor callback) {
		val mountRootName = nodeNameEncoder.encode("");
		val visitor = new JavaNIOToNodeFsDataFileVisitor(mountRootName, nodeNameEncoder, callback);
		try {
			Files.walkFileTree(rootScanDir, visitor);
		} catch (IOException e) {
			throw new RuntimeException("Failed", e);
		}
	}
	
	@RequiredArgsConstructor
	private static class DirNodeFsDataBuilder {
		private final NodeNamesPath path;
		private final long creationTime;
		private final long lastModifiedTime;
		private final ImmutableMap<String,Object> extraFsAttrs;
		// private final TreeMap<NodeName,DirEntryNameAndType> childEntries = new TreeMap<NodeName,DirEntryNameAndType>();
		private final TreeSet<NodeName> childNames = new TreeSet<NodeName>();
	}
	
	protected static class JavaNIOToNodeFsDataFileVisitor extends SimpleFileVisitor<Path> {
		private final NodeName rootName; // HACK... name is set in NodeData, but not in Tree.rootNode entry (immutable..)
		private final NodeNameEncoder nodeNameEncoder;
		private final NodeFsDataVisitor callback;
		
		private final List<DirNodeFsDataBuilder> currDirBuilderStack = new ArrayList<>();
		private DirNodeFsDataBuilder currDirBuilder;
		
		protected JavaNIOToNodeFsDataFileVisitor(NodeName rootName, NodeNameEncoder nodeNameEncoder, NodeFsDataVisitor callback) {
			this.rootName = rootName;
			this.nodeNameEncoder = nodeNameEncoder;
			this.callback = callback;
			// currDirBuilder = new DirNodeFsDataBuilder(NodeNamesPath.ROOT, 0, 0, null);
			// currDirBuilderStack.add(currDirBuilder);
		}
		
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			String fileName;
			try {
				fileName = dir.getFileName().toString();
			} catch(NullPointerException ex) {
				fileName = dir.toString(); // for root "c:" ?!
			}
			val name = nodeNameEncoder.encode(fileName);
			val path = (currDirBuilder != null)? currDirBuilder.path.toChild(name) : NodeNamesPath.ROOT;

			long creationTime = attrs.creationTime().toMillis();
			long lastModifiedTime = attrs.lastModifiedTime().toMillis();
			val extraFsAttrs = ImmutableMap.<String,Object> of(
					// TOADD
					);

			DirNodeFsDataBuilder dirBuilder = new DirNodeFsDataBuilder(path, creationTime, lastModifiedTime, extraFsAttrs);

			if (currDirBuilder == null) {
				currDirBuilder = new DirNodeFsDataBuilder(NodeNamesPath.ROOT, 0, 0, null);
			}
			currDirBuilderStack.add(dirBuilder);
			this.currDirBuilder = dirBuilder;
			
			return super.preVisitDirectory(dir, attrs);
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			// pop and add dir to parent
			boolean isRoot = (currDirBuilder.path.size() == 0); 
			NodeName name = (isRoot)? rootName : currDirBuilder.path.last();
			NodeNamesPath path = (isRoot)? NodeNamesPath.ROOT : currDirBuilder.path;
			DirNodeFsData dirNode = new DirNodeFsData(name, 
					currDirBuilder.creationTime, currDirBuilder.lastModifiedTime, currDirBuilder.extraFsAttrs,
					currDirBuilder.childNames);
			callback.caseDir(path, dirNode);
				
			int level = currDirBuilderStack.size() - 1;
			currDirBuilderStack.remove(level);
			if (level > 0) {
				val parentNode = currDirBuilderStack.get(level-1);
				this.currDirBuilder = parentNode;
				// parentNode.childEntries.put(dirNode.name, new DirEntryNameAndType(dirNode.name, FsNodeType.DIR));
				parentNode.childNames.add(dirNode.name);
			}
			
			return super.postVisitDirectory(dir, exc);
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			String fileName = file.getFileName().toString();
			val name = nodeNameEncoder.encode(fileName);
			val path = currDirBuilder.path.toChild(name);
			
			NodeFsData resNode = null;

			long creationTime = attrs.creationTime().toMillis();
			long lastModifiedTime = attrs.lastModifiedTime().toMillis();
			
			if (attrs.isRegularFile()) {
				long fileLength = attrs.size();
				val extraFsAttrs = ImmutableMap.<String,Object>of(
						// TOADD
						);
				
				val fileNode = new FileNodeFsData(name, creationTime, lastModifiedTime, extraFsAttrs, fileLength);
				resNode = fileNode;
				
				callback.caseFile(path, fileNode);

			} else if (attrs.isDirectory()) {
				// should not occur
			} else if (attrs.isSymbolicLink()) {
				// TODO unsupported yet ..
				resNode = null;
			} else if (attrs.isOther()) {
				// unrecognized?!
				resNode = null;
			} else {
				// should not occur
				resNode = null;
			}

			if (resNode != null) {
				if (attrs instanceof PosixFileAttributeView) {
//			    UserPrincipal owner();
//			    GroupPrincipal group();
//			    Set<PosixFilePermission> permissions();
				}
				
				// this.currDirBuilder.childEntries.put(name, new DirEntryNameAndType(name, FsNodeType.FILE));
				this.currDirBuilder.childNames.add(name);
			}
			
			return FileVisitResult.CONTINUE;
		}
		
		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			log.error("Failed " + file, exc);
			return FileVisitResult.CONTINUE;
		}

	}
}
