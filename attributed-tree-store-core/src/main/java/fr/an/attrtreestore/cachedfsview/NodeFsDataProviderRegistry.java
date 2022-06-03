package fr.an.attrtreestore.cachedfsview;

import java.util.ArrayList;
import java.util.List;

import lombok.val;

public class NodeFsDataProviderRegistry extends NodeFsDataProviderFactory {
	
	public static final NodeFsDataProviderRegistry DEFAULT = new NodeFsDataProviderRegistry();
	
	private List<NodeFsDataProviderFactory> providerFactories = new ArrayList<>();

	// ------------------------------------------------------------------------
	
	public NodeFsDataProviderRegistry() {
	}

	public NodeFsDataProviderRegistry(List<NodeFsDataProviderFactory> providerFactories) {
		if (providerFactories != null && !providerFactories.isEmpty()) {
			this.providerFactories.addAll(providerFactories);
		}
	}

	// ------------------------------------------------------------------------
	
	public void register(NodeFsDataProviderFactory factory) {
		this.providerFactories.add(factory);
	}

	public void unregister(NodeFsDataProviderFactory factory) {
		this.providerFactories.remove(factory);
	}
	
	// implements NodeFsDataProviderFactory
	// ------------------------------------------------------------------------

	@Override
	public boolean match (String baseUrl) {
		for(val factory: providerFactories) {
			boolean match = factory.match(baseUrl);
			if (match) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public NodeFsDataProvider create(String baseUrl) {
		for(val factory: providerFactories) {
			boolean match = factory.match(baseUrl);
			if (match) {
				val res = factory.create(baseUrl);
				if (res != null) {
					return res;
				}
			}
		}
		return null;
	}

}
