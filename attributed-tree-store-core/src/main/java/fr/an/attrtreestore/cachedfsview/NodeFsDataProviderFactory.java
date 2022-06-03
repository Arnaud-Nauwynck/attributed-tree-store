package fr.an.attrtreestore.cachedfsview;

public abstract class NodeFsDataProviderFactory {
	
	public abstract boolean match(String baseUrl);
	
	public abstract NodeFsDataProvider create(String baseUrl);
	
}
