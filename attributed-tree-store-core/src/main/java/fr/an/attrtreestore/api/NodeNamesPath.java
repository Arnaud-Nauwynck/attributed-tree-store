package fr.an.attrtreestore.api;

import java.util.Arrays;

import lombok.val;

/**
 * value-object path, represented as NodeName[]
 *
 */
public final class NodeNamesPath {

	public static final NodeNamesPath ROOT = new NodeNamesPath(new NodeName[0]);
	
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

	public NodeNamesPath toChild(NodeName childName) {
		val len = pathElements.length; 
		val res = new NodeName[len + 1];
		System.arraycopy(pathElements, 0, res, 0, len);
		res[len] = childName;
		return new NodeNamesPath(res);
	}

	public boolean startsWith(NodeName name) {
		if (pathElements.length < 1) {
			return false;
		}
		return pathElements[0].equals(name);
	}

	/**
	 * @param start len to prune
	 * @return pruned path, example "a/b/c"  pruneStartPath(1) -> "b/c"
	 */
	public NodeNamesPath pruneStartPath(int start) {
		val len = pathElements.length; 
		val res = new NodeName[len - start];
		System.arraycopy(pathElements, start, res, 0, len-start);
		return new NodeNamesPath(res);
	}
	
	public NodeNamesPath subPath(int len) {
        val res = new NodeName[len];
        System.arraycopy(pathElements, 0, res, 0, len);
        return new NodeNamesPath(res);
    }
	
	public NodeNamesPath toParent() {
		val len = pathElements.length;
		if (len == 0) {
			// error?
			return this;
		}
		val res = new NodeName[len - 1];
		System.arraycopy(pathElements, 0, res, 0, len - 1);
		return new NodeNamesPath(res);
	}

    public static NodeNamesPath commonPathOf(NodeNamesPath left, NodeNamesPath  right) {
        val leftPathElements = left.pathElements;
        val rightPathElements = right.pathElements;
        val maxLen = Math.min(leftPathElements.length, rightPathElements.length);
        int i = 0;
        for(; i < maxLen; i++) {
            if (! leftPathElements[i].equals(rightPathElements[i])) {
                break;
            }
        }
        return left.subPath(i);
    }
    
	// ------------------------------------------------------------------------

	public int pathElementCount() {
		return pathElements.length;
	}

	public NodeName pathElement(int i) {
        return pathElements[i];
    }
	
	public NodeName lastName() {
		return pathElements[pathElements.length-1];
	}

	public NodeName lastNameOrEmpty() {
		if (pathElements.length == 0) return NodeName.EMPTY;
		return pathElements[pathElements.length-1];
	}

	
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

	public String[] toTexts() {
		val len = pathElements.length;
		val res = new String[len];
		for(int i = 0; i < len; i++) {
			res[i] = pathElements[i].toText();
		}
		return res;
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
