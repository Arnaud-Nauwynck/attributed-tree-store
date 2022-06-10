package fr.an.attrtreestore.util;

import fr.an.attrtreestore.util.LoggingCounter.LoggingCounterParams.LoggingCounterParamsBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

public class LoggingCounter {

	@Builder
	@AllArgsConstructor
	public static class LoggingCounterParams {
	
		@Builder.Default
		@Getter @Setter
		private int logFreq = 1000;
	
		@Builder.Default
		@Getter @Setter
		private int logMaxDelayMillis = 30 * 1000;
	
	}
	
	@FunctionalInterface
	public static interface MsgPrefixLoggingCallback {
//		public void logMessage(int count, long millis, // 
//				long elapsedMillisSinceLastLog, int countSinceLastLog, long sumMillisSinceLastLog, // 
//				String displayName);
		public void logWithMessagePrefix(String msgPrefix);
	}
	
	private final String displayName;
	
	@Getter @Setter
	private int logFreq = 1000;

	@Getter @Setter
	private int logMaxDelayMillis = 30 * 1000;

	@Getter
	private int count;
	@Getter
	private long totalMillis;

	
	private int countLogModuloFreq = 0;
	
	private long lastLogTimeMillis;
	private int countSinceLastLog;
	private long sumMillisSinceLastLog;
	
	// ------------------------------------------------------------------------
	
	public LoggingCounter(String displayName) {
		this(displayName, LoggingCounterParams.builder());
	}

	public LoggingCounter(String displayName, LoggingCounterParamsBuilder params) {
		this(displayName, params.build());
	}

	public LoggingCounter(String displayName, LoggingCounterParams params) {
		this.displayName = displayName;
		this.logFreq = params.logFreq;
		this.logMaxDelayMillis = params.logMaxDelayMillis;
	}

	// ------------------------------------------------------------------------
	
	public synchronized void incr(long millis, MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		this.count++;
		this.totalMillis += millis;
		this.countLogModuloFreq--;
		this.countSinceLastLog++;		
		this.sumMillisSinceLastLog += millis;
		boolean log = false;
		if (countLogModuloFreq <= 0) {
			this.countLogModuloFreq = logFreq;
			log = true;
		}
		val now = System.currentTimeMillis();
		val elapsed =  now - lastLogTimeMillis;
		log = log || (elapsed > logMaxDelayMillis);
		if (log) {
			val msgPrefix = "(..+" + countSinceLastLog + " = " + count //
					+ " + " + durationToString(sumMillisSinceLastLog) //
					+ ", since " + durationToString(elapsed) + ")" //
					+ " " + displayName;
			
			msgPrefixLoggingCallback.logWithMessagePrefix(msgPrefix);
			
			this.countSinceLastLog = 0;
 			this.sumMillisSinceLastLog = 0;
 			this.lastLogTimeMillis = now; 
		}
	}

	protected static String durationToString(long millis) {
		if (millis < 1000) {
			return millis + "ms";
		} else {
			val seconds = millis/1000;
			val remainMillis = millis - 1000 * seconds;
			String res = "";
			if (seconds <= 2 && remainMillis > 50) {
				res = remainMillis + "ms"; 
			}// else neglectable millis
			val minutes = seconds / 60;
			val remainSeconds = seconds - minutes * 60; 
			if (minutes <= 2 && remainSeconds > 1) {
				res = remainSeconds + "s" + res;
			}// else neglectable seconds
			if (minutes != 0) {
				res = minutes + "mn" + res; 
			}
			return res;
		}
	}
	
}
