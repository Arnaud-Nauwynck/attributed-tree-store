package fr.an.attrtreestore.api.attrinfo;

import java.io.DataInput;
import java.io.DataOutput;

public abstract class AttrDataEncoder<T> {

	public abstract void writeData(DataOutput out, T data);

	public abstract T readData(DataInput in);

}
