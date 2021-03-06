package fr.an.attrtreestore.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Collection;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import lombok.extern.slf4j.Slf4j;

/**
 * This memory warning system will call the listener when we exceed the
 * percentage of available memory specified. There should only be one instance
 * of this object created, since the usage threshold can only be set to one
 * number.
 * 
 * ( adapted from http://www.javaspecialists.eu/archive/Issue092.html )
 */
@Slf4j
public class MemoryWarningSystem {

    public interface Listener {

        void memoryUsageLow(long usedMemory, long maxMemory);
    }

    private final Collection<Listener> listeners = new ArrayList<Listener>();

	private double percentageUsageThreshold;
	private long prevMaxMemory;
	
    private static final MemoryPoolMXBean tenuredGenPool = findTenuredGenPool();

    public static final MemoryWarningSystem instance = new MemoryWarningSystem();
    
    public MemoryWarningSystem() {
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        NotificationEmitter emitter = (NotificationEmitter) mbean;
        emitter.addNotificationListener(new NotificationListener() {
            @Override
            public void handleNotification(Notification n, Object hb) {
                if (n.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
                    long maxMemory = tenuredGenPool.getUsage().getMax();
                    long usedMemory = tenuredGenPool.getUsage().getUsed();
                    if (maxMemory != prevMaxMemory) {
                    	updateTenuredGenPoolUsageThreshold();
                    }
                    log.info("detected memory used " + (usedMemory/1024/1024) + " Mb"
                    		+ ",  exceeded threshold " + percentageUsageThreshold 
                    		+ " of max:" + (maxMemory/1024/1024) + " Mb");
                    for (Listener listener : listeners) {
                        listener.memoryUsageLow(usedMemory, maxMemory);
                    }
                }
            }
        }, null, null);
    }

    public boolean addListener(Listener listener) {
        return listeners.add(listener);
    }

    public boolean removeListener(Listener listener) {
        return listeners.remove(listener);
    }

    public void setPercentageUsageThreshold(double percentage) {
        if (percentage <= 0.0 || percentage > 1.0) {
            throw new IllegalArgumentException("Percentage not in range");
        }
        this.percentageUsageThreshold = percentage;
        updateTenuredGenPoolUsageThreshold();
    }

	private void updateTenuredGenPoolUsageThreshold() {
		long maxMemory = tenuredGenPool.getUsage().getMax();
		this.prevMaxMemory = maxMemory;
        long warningThreshold = (long) (maxMemory * percentageUsageThreshold);
        tenuredGenPool.setUsageThreshold(warningThreshold);
	}

    /**
     * Tenured Space Pool can be determined by it being of type HEAP and by it
     * being possible to set the usage threshold.
     */
    private static MemoryPoolMXBean findTenuredGenPool() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            // I don't know whether this approach is better, or whether
            // we should rather check for the pool name "Tenured Gen"?
            if (pool.getType() == MemoryType.HEAP
                    && pool.isUsageThresholdSupported()) {
                return pool;
            }
        }
        throw new IllegalStateException("Could not find tenured space");
    }
}