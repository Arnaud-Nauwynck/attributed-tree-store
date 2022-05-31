package fr.an.attrtreestore.api;

import java.util.Arrays;

import fr.an.attrtreestore.api.name.NodeNameEncoder;
import lombok.val;

/**
 * value-object path, represented as NodeName[]
 *
 */
public final class NodeNamesPath {

	public final NodeName[] pathElements;
	
	// private int hashCode;
	
	// ------------------------------------------------------------------------
	
	/** absolute pathes should be obtained from NodeNameEncoder.encodePath() */
	public NodeNamesPath(NodeName[] pathElements) {
		this.pathElements = pathElements;
	}

	public static NodeNamesPath of(NodeName... pathElements) {
		return new NodeNamesPath(pathElements);
	}

	// ------------------------------------------------------------------------

	public String toPathSlash() {
		StringBuilder sb = new StringBuilder(50 + 20 * pathElements.length);
		val len = pathElements.length;
		for(int i = 0; i < len; i++) {
			pathElements[i].appendTo(sb);
			if (i + 1 < len) {
				sb.append("/");
			}
		}
		return sb.toString();
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(pathElements);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeNamesPath other = (NodeNamesPath) obj;
		return Arrays.equals(pathElements, other.pathElements);
	}

	@Override
	public String toString() {
		return toPathSlash();
	}
	
}
