package fr.an.attrtreestore;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TstMkdirDirUtils {

	public static File initMkdir(String dir) {
		File res = new File(dir);
		ensureMkDirEmpty(res);
		return res;
	}

	public static File initMkdir(File parentDir, String dirName) {
		File res = new File(parentDir, dirName);
		ensureMkDirEmpty(res);
		return res;
	}
	
	public static void ensureMkDirEmpty(File dir) {
		if (dir.exists()) {
			// remove content
			File[] childFiles = dir.listFiles();
			for(val childFile: childFiles) {
				recursiveDelete(childFile);
			}
		} else {
			dir.mkdirs();
		}
	}
	
	public static void recursiveDelete(File file) {
		log.info("delete " + file);
		if (file.isFile()) {
			file.delete();
		} else {
			File[] childFiles = file.listFiles();
			for(val childFile: childFiles) {
				// *** recurse ***
				recursiveDelete(childFile);
			}
		}
	}
}
