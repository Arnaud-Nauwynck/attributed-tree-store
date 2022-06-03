package fr.an.attrtree.hadoop3;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import fr.an.attrtreestore.api.name.NodeNameEncoder;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProvider;
import fr.an.attrtreestore.cachedfsview.NodeFsDataProviderFactory;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HadoopNodeFsDataProviderFactory extends NodeFsDataProviderFactory {

	private final Configuration hadoopConf;
	private final NodeNameEncoder nodeNameEncoder;
	
	// may add specific provider instead?
	private final boolean matchAbfss;
	
	// use when fs.defaultFS = "hdfs:///XX"
	private final boolean matchDefaultStartSlash; 
	
	@Override
	public NodeFsDataProvider create(String baseUrl) {
		URI baseURI = createURI(baseUrl);
		FileSystem fs;
		try {
			fs = FileSystem.get(baseURI, hadoopConf);
		} catch (IOException ex) {
			throw new RuntimeException("Failed FileSystem.get(" + baseURI + ")", ex);
		}
		return new HadoopNodeFsDataProvider(baseUrl, fs, nodeNameEncoder);
	}

	public static URI createURI(String uri) {
		try {
			return new URI(uri);
		} catch (URISyntaxException ex) {
			throw new IllegalArgumentException("bad URI '" + uri + "'", ex);
		}
	}

	@Override
	public boolean match(String baseUrl) {
		return baseUrl.startsWith("hdfs://")
				|| (matchAbfss && baseUrl.startsWith("abfss://"))
				|| (matchDefaultStartSlash && baseUrl.startsWith("/"));
	}

}
