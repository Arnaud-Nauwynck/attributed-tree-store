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
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import fr.an.attrtreestore.api.NodeName;
import fr.an.attrtreestore.api.NodeNamesPath;
import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.util.fsdata.DirEntryNameAndType;
import fr.an.attrtreestore.util.fsdata.FsNodeType;
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
		val visitor = new JavaNIOToNodeFsDataFileVisitor(nodeNameEncoder, callback);
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
		private final TreeMap<NodeName,DirEntryNameAndType> childEntries = new TreeMap<NodeName,DirEntryNameAndType>();
	}
	
	protected static class JavaNIOToNodeFsDataFileVisitor extends SimpleFileVisitor<Path> {
		private final NodeNameEncoder nodeNameEncoder;
		private final NodeFsDataVisitor callback;
		
		private final List<DirNodeFsDataBuilder> currDirBuilderStack = new ArrayList<>();
		private DirNodeFsDataBuilder currDirBuilder;
		
		protected JavaNIOToNodeFsDataFileVisitor(NodeNameEncoder nodeNameEncoder, NodeFsDataVisitor callback) {
			this.nodeNameEncoder = nodeNameEncoder;
			this.callback = callback;
			currDirBuilder = new DirNodeFsDataBuilder(NodeNamesPath.ROOT, 0, 0, null);
			currDirBuilderStack.add(currDirBuilder);
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
			val path = currDirBuilder.path.toChild(name);

			long creationTime = attrs.creationTime().toMillis();
			long lastModifiedTime = attrs.lastModifiedTime().toMillis();
			val extraFsAttrs = ImmutableMap.<String,Object> of(
					// TOADD
					);

			DirNodeFsDataBuilder dirBuilder = new DirNodeFsDataBuilder(path, creationTime, lastModifiedTime, extraFsAttrs);

			currDirBuilderStack.add(dirBuilder);
			this.currDirBuilder = dirBuilder;
			
			return super.preVisitDirectory(dir, attrs);
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			DirNodeFsData dirNode = new DirNodeFsData(currDirBuilder.path.lastName(), 
					currDirBuilder.creationTime, currDirBuilder.lastModifiedTime, currDirBuilder.extraFsAttrs,
					currDirBuilder.childEntries);
			callback.caseDir(currDirBuilder.path, dirNode);
			
			// pop and add dir to parent
			int level = currDirBuilderStack.size() - 1;
			currDirBuilderStack.remove(level);
			
			val parentNode = currDirBuilderStack.get(level-1);
			this.currDirBuilder = parentNode;
			parentNode.childEntries.put(dirNode.name, new DirEntryNameAndType(dirNode.name, FsNodeType.DIR));
			
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
				
				this.currDirBuilder.childEntries.put(name, new DirEntryNameAndType(name, FsNodeType.FILE));
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
