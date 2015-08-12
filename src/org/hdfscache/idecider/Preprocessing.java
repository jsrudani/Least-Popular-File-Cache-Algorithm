package org.hdfscache.idecider;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.TimerTask;

/**
 * This class is used to preprocess the trace file. It is used to prepare Map of
 * <Timestamp, Task>. Timestamp signifies the time at which operation is
 * performed on file and Task is Runnable instance which includes all the task
 * performed on a file at Timestamp
 * 
 * @author jsrudani
 * 
 */
public class Preprocessing {

    /**
     * It is used to hold the file name on which preprocessing is done. It
     * represents absolute path for file
     */
    private final String fileName;

    /**
     * This represent the List of Entry of File name and Operation on Filename
     */
    private List<LPFEntry<String, String>> filenameNOperationEntryList = null;

    /**
     * This represents the timestamp value from which other timestamps are
     * subtracted
     */
    private static long minuend = 0;

    /**
     * This represents the time between two transaction
     */
    private static long interArrivalJobtime = 0L;

    /**
     * This represents Map of tasks performed at given timestamp
     */
    private List<LPFEntry<Long, TimerTask>> taskPerTimestampList = new ArrayList<LPFEntry<Long, TimerTask>>();

    /**
     * It represents type of cache requested by user
     */
    private final Cache cache;

    public Preprocessing(String filename, Cache cache) {
        this.fileName = filename;
        this.cache = cache;
    }

    /**
     * This method is used to read and process each transaction in trace file
     * 
     * @throws IOException
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     */
    public void readAndProcessTrace()
            throws FileNotFoundException, IOException,
            IllegalArgumentException {
        System.out.println("Reading file :" + fileName);
        try (FileInputStream fin = new FileInputStream(fileName);
                Scanner scan = new Scanner(fin, "UTF-8")) {
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                processLine(line.split("\\s+"));
            }
            if (filenameNOperationEntryList != null) {
                // Store the current list with time
                buildTimestampTransactionList(interArrivalJobtime, filenameNOperationEntryList);
            }
        }
        System.out.println("Finished reading and processing file :" + fileName);
    }

    /**
     * This method is used to process line and extract Timestamp, File name and
     * Operation performed on File. It also prepares the Map<Timestamp,Task>.
     * Fields has specific order [Timestamp Filename Operation]
     * 
     * @param fields
     *            Represents Array of fields seperated by space\tab
     * @throws IllegalArgumentException
     */
    private void processLine(String[] fields)
            throws IllegalArgumentException {
        // System.out.println("Processing single line with fields " +
        // fields.length);
        if (fields.length != 3) {
            throw new IllegalArgumentException("Less/More number of fields in File");
        }
        long currentTimestamp = Long.valueOf(fields[0]);
        String filename = fields[1];
        String operation = fields[2];
        // System.out.println(currentTimestamp + "," + filename + "," +
        // operation);

        // Check if repeated timestamp
        if ((minuend - currentTimestamp) == 0) {
            filenameNOperationEntryList.add(new LPFEntry<String, String>(filename, operation));
        } else {
            if (filenameNOperationEntryList != null) {
                // Store the current list with time
                buildTimestampTransactionList(interArrivalJobtime, filenameNOperationEntryList);
                interArrivalJobtime = (currentTimestamp - minuend);
            }
            filenameNOperationEntryList = new ArrayList<LPFEntry<String, String>>();
            filenameNOperationEntryList.add(new LPFEntry<String, String>(filename, operation));
            minuend = currentTimestamp;
        }
    }

    private void buildTimestampTransactionList(
            long startTime,
            List<LPFEntry<String, String>> fileNameOperationEntries) {
        // System.out.println("List prepared for timestamp :" + startTime +
        // " is :" + fileNameOperationEntries);
        taskPerTimestampList.add(new LPFEntry<Long, TimerTask>(startTime, new FileOperationPerTimestampTask(fileNameOperationEntries, cache)));
    }

    /**
     * This returns the immutable view of task per timestamp list
     * 
     * @return Collections Immutable List
     */
    public List<LPFEntry<Long, TimerTask>> getTaskPerTimestampList() {
        return Collections.unmodifiableList(taskPerTimestampList);
    }
}
