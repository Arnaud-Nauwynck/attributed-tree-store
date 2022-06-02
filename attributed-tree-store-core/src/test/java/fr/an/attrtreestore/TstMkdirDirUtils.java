package fr.an.attrtreestore;

import java.io.File;

public class TstMkdirDirUtils {

	public static File initMkdir(String dir) {
		File res = new File(dir);
		res.mkdirs();
		return res;
	}
}
