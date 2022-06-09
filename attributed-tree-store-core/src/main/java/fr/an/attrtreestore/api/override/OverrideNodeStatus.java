package fr.an.attrtreestore.api.override;

public enum OverrideNodeStatus {
	DELETED,   // tombstone deletion in cache depending of context?
	NOT_OVERRIDEN, // or NOT_CACHED depending of context?
	UPDATED;  // or IN_CACHE depending of context?
}
