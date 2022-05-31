package fr.an.attrtreestore.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import fr.an.attrtreestore.storage.NodeTreeLoader;

/**
 * Tree of Node, either in-memory or cached on storage
 *
 * example of sub-classes implementations:
 * <ul>
 *   <li> pure in-memory Node tree ... might suit only for small hierarchical tree and huge jvm memory
 *   </li>
 *   
 *   <li> using custom serialized file:
 *      init file: fully recursive dump of tree (may or not contain address for off-heap lookup)
 *      then file2...N: append only dump of individual nodes, for updates (or thombstone for node deletion)
 *   </li>
 *   
 *   <li> using LevelDB:  path -> Serialized Node Data (including list of childName, so can lookup any child by name, without doing scan)
 *   </li>
 *   
 * </ul>
 */
public abstract class NodeTree {

	public abstract NodeAccessor rootNode();
	
	/*pp*/ abstract NodeTreeLoader nodeTreeLoader();
	
	
	public abstract CompletableFuture<NodeAccessor> asyncResolvePath(String path);
	
	/** blocking helper for asyncResolvePath() */
	public NodeAccessor resolvePath(String path) {
		try {
			return asyncResolvePath(path).get();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Failed resolvePath " + path, ex);
		} catch (ExecutionException ex) {
			throw new RuntimeException("Failed resolvePath " + path, ex);
		}
	}

	
}
