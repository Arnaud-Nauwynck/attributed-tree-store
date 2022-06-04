package fr.an.attrtreestore.util;

import java.util.concurrent.ThreadFactory;

import lombok.val;

public class DefaultNamedTreadFactory implements ThreadFactory {
	
	private final String namePrefix;
	private final String nameSuffix;
	private final boolean isDaemon;
	
	private int threadNumSequence = 1;

	// ------------------------------------------------------------------------
	
	public DefaultNamedTreadFactory(String namePrefix, String nameSuffix, boolean isDaemon) {
		this.namePrefix = namePrefix;
		this.nameSuffix = nameSuffix;
		this.isDaemon = isDaemon;
	}

	// ------------------------------------------------------------------------

	@Override
	public Thread newThread(Runnable r) {
		int threadNum;
		synchronized(this) {
			threadNum = threadNumSequence++;
		}
		val res = new Thread(namePrefix + threadNum + nameSuffix);
		res.setDaemon(isDaemon);
		return res;
	}

}
