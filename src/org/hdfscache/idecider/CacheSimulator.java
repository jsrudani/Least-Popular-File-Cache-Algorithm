package org.hdfscache.idecider;


/**
 * This class is main application class. It is used to read/process/replay
 * traces.
 * 
 * @author jsrudani
 * 
 */
public class CacheSimulator {

    public static void main(String[] args) {
        try {
            System.out.println("Starting cache simulator");
            if (args.length != 2) {
                throw new IllegalArgumentException("Wrong number of Parameters !!!");
            }
            // Need to add check for file existent
            String filename = args[0];
            String cacheType = args[1];
            // Check which type of cache is requested and delegate the request
            // to that cache implementation
            Cache cache = checkAndReturnCacheReference(cacheType);
            // Reading and Pre-processing steps
            Preprocessing preprocess = new Preprocessing(filename, cache);
            preprocess.readAndProcessTrace();
            System.out.println(preprocess.getTaskPerTimestampList());
            // Replay traces prepared after pre-processing
            ReplayTraces replayTrace = new ReplayTraces(preprocess.getTaskPerTimestampList());
            replayTrace.runTransactionAsPerArrivalTime();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
        }
    }

    private static Cache checkAndReturnCacheReference(
            String cacheType) {
        Cache delegate = null;
        switch (cacheType) {
            case LPFConstant.LPF_CACHE_TYPE:
                delegate = new LPFCache();
                break;
            default:
                throw new IllegalArgumentException("Unknown Cache");
        }
        return delegate;
    }
}
