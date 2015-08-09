package org.hdfscache.idecider;

/**
 * This class is main application class. It is used to read/process/replay
 * traces.
 * 
 * @author jsrudani
 * 
 */
public class LPFCacheSimulator {

    public static void main(String[] args) {
        try {
            System.out.println("Starting cache simulator");
            if (args.length != 1) {
                throw new IllegalArgumentException("Wrong number of Parameters !!!");
            }
            String filename = args[0];
            // Need to add check for file existent
            // Reading and Pre-processing steps
            Preprocessing preprocess = new Preprocessing(filename);
            preprocess.readAndProcessTrace();
            System.out.println(preprocess.getTaskPerTimestampList());
            // Replay traces prepared after pre-processing
            ReplayTraces replayTrace = new ReplayTraces(preprocess.getTaskPerTimestampList());
            replayTrace.runTransactionAsPerArrivalTime();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
        }
    }
}
