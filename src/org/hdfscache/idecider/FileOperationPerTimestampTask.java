package org.hdfscache.idecider;

import java.util.Iterator;
import java.util.List;
import java.util.TimerTask;

public class FileOperationPerTimestampTask extends
        TimerTask {

    private final List<LPFEntry<String, String>> filenameNOperationEntryList;

    FileOperationPerTimestampTask(
            List<LPFEntry<String, String>> filenameNOperationEntryList) {
        this.filenameNOperationEntryList = filenameNOperationEntryList;
    }

    @Override
    public String toString() {
        return "[" + filenameNOperationEntryList + "]";
    }

    @Override
    public void run() {
        try {
            // Traverse the list and execute transaction
            Iterator<LPFEntry<String, String>> transactionItr = filenameNOperationEntryList.iterator();
            while (transactionItr.hasNext()) {
                executeTransaction(transactionItr.next());
            }
        } catch (Exception ex) {
            System.out.println("Problem in executing transaction " + ex.getMessage());
        } finally {
            this.cancel();
        }
    }

    /**
     * It is used to check which operation is performed on file and then execute
     * that operation
     * 
     * @param transaction
     * @throws Exception
     */
    private void executeTransaction(
            LPFEntry<String, String> transaction)
            throws Exception {
        String operation = transaction.getValue();
        String filename = transaction.getKey();
        switch (operation) {
            case LPFConstant.FILE_OPEN:
                FileOperation.open(filename);
                break;
            case LPFConstant.FILE_CREATE:
                FileOperation.create(filename);
                break;
            default:
                System.out.println("Unknown File operation");
                break;
        }
    }

}
