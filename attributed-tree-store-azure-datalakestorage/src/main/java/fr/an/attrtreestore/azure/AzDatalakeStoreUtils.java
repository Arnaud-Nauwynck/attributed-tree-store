package fr.an.attrtreestore.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.models.PathItem;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import fr.an.attrtreestore.util.LoggingCounter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzDatalakeStoreUtils {

	public static String pathItemToChildName(PathItem pathItem) {
	    String res;
	    String azChildPath = pathItem.getName(); // path, not childName... extract
	    int lastSlash = azChildPath.lastIndexOf('/');
	    if (lastSlash == -1 || lastSlash == 0) {
	        res = azChildPath;
	    } else {
	        res = (lastSlash < azChildPath.length())? azChildPath.substring(lastSlash+1) : "";
	    }
	    return res;
	}

	public static List<PathItem> retryableAzQueryListPaths(DataLakeDirectoryClient dirClient, LoggingCounter counter, int maxRetry) {
        List<PathItem> res;
        int retryCount = 0;
        for(;; retryCount++) {
            try {
                Duration timeout = Duration.ofMinutes(5);

                long startTime = System.currentTimeMillis();
                
                // *** az query listPaths (first page) ***
                PagedIterable<PathItem> pathItemsIterable = dirClient.listPaths(false, false, null, timeout);
                res = new ArrayList<>();
                for(PathItem azChildPathItem : pathItemsIterable) { // *** az query list more page ***
                    res.add(azChildPathItem);
                }
                
                long millis = System.currentTimeMillis() - startTime;
                counter.incr(millis, msgPrefix -> log.info(msgPrefix + " " + dirClient.getDirectoryPath()));
                
                break;
            } catch(RuntimeException ex) {
                if (retryCount + 1 < maxRetry) {
                    log.error("Failed az query listPaths: " + dirClient.getDirectoryUrl() + " .. retry [" + retryCount + "/" + maxRetry + "] ex:" + ex.getMessage());
                    sleepAfterNRetry(retryCount);
                } else {
                    log.error("Failed az query listPaths: " + dirClient.getDirectoryUrl() + " .. rethrow " + ex.getMessage());
                    throw ex;
                }
            }
        }
        return res;
    }

    public static void sleepAfterNRetry(int retryCount) {
        try {
            Thread.sleep(500 + retryCount * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static long toMillis(OffsetDateTime src) {
        if (src == null) {
            return 0;
        }
        long res = src.toInstant().toEpochMilli();
        return res;
    }

}
