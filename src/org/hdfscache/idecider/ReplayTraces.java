package org.hdfscache.idecider;

import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class is used to schedule the transaction batched as per arrival time.
 * 
 * @author jsrudani
 * 
 */
public class ReplayTraces {

    private final List<LPFEntry<Long, TimerTask>> batchedTransactionPerTimestamp;
    private Timer startJobTimer;

    public ReplayTraces(
            List<LPFEntry<Long, TimerTask>> batchedTransactionPerTimestamp) {
        this.batchedTransactionPerTimestamp = batchedTransactionPerTimestamp;
        startJobTimer = new Timer();
    }

    /**
     * It is used to schedule the batch as per its arrival time.
     * 
     * @throws Exception
     */
    public void runTransactionAsPerArrivalTime() {
        try {
            Iterator<LPFEntry<Long, TimerTask>> startJobPerTimestampItr = batchedTransactionPerTimestamp.iterator();
            long startTime = 0L;
            while (startJobPerTimestampItr.hasNext()) {
                LPFEntry<Long, TimerTask> batchEntry = startJobPerTimestampItr.next();
                startTime += batchEntry.getKey();
                startJobTimer.schedule(batchEntry.getValue(), startTime);
            }
        } catch (Exception ex) {
            System.out.println("Problem in executing transaction " + ex.getMessage());
        }
    }
}
