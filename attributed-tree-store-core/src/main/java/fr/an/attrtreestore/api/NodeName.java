package fr.an.attrtreestore.api;

public abstract class NodeName implements Comparable<NodeName> {

	public abstract String toText();

	// ------------------------------------------------------------------------

	@Override
	public int compareTo(NodeName other) {
		String text = toText();
		String otherText = other.toText();
		return text.compareTo(otherText);
	}

	@Override
	public String toString() {
		return toText();
	}

	@Override
	public int hashCode() {
		return toText().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeName other = (NodeName) obj;
		return toText().equals(other.toText());
	}
	
	
}
