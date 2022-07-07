package fr.an.attrtreestore.api.name;

import fr.an.attrtreestore.api.NodeName;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StringNodeName extends NodeName {

	public final String name;
	
	@Override
	public String toText() {
		return name;
	}
}