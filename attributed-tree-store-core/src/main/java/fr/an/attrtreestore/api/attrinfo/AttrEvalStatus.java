package fr.an.attrtreestore.api.attrinfo;

public enum AttrEvalStatus {
	DIRTY,
	PENDING,
	OK;
	
	
	public byte toByte() {
		switch(this) {
		case DIRTY: return 'd';
		case PENDING: return 'p';
		case OK: return 'o';
		default: throw new RuntimeException();
		}
	}

	public static AttrEvalStatus fromByte(byte b) {
		switch(b) {
		case 'd': return DIRTY;
		case 'p': return PENDING;
		case 'o': return OK;
		default: throw new RuntimeException();
		}
	}

}