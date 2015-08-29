package org.hdfscache.idecider;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents file operation like open/close/create/delete
 * 
 * @author jsrudani
 * 
 */
public class FileOperation {

    /***
     * It represents the mapping of filename -> Inode. It is thread safe and
     * efficient since concurrent hash map locks only certain region of Map.
     */
    private static Map<String, Inode> fileToInodeMap = new ConcurrentHashMap<String, Inode>();

    /**
     * It performs file read operation for given filename
     * 
     * @param filename
     *            The file to be read
     * @throws IllegalArgumentException
     */
    public static void open(String filename, Cache cache)
            throws IllegalArgumentException {
        System.out.println("Open " + filename + " at " + System.currentTimeMillis());
        // Check if file is created or not
        if (fileToInodeMap.containsKey(filename)) {
            Inode fileInodeInfo = fileToInodeMap.get(filename);
            // Call the respective cache implementation of read operation
            cache.read(fileInodeInfo);
            // System.out.println("Valid Inode " +
            // fileToInodeMap.get(filename));
        } else {
            System.out.println("Since file is not created so Open is invalid for file " + filename);
        }
    }

    /**
     * It is used to create file metadata. It initialize the inode structure for
     * given file. If any error in creating inode it throws exception. Inode by
     * default contains default value.
     * 
     * @param filename
     * @throws Exception
     */
    public static void create(String filename)
            throws Exception {
        long creationtime = System.currentTimeMillis();
        System.out.println("Create " + filename + " at " + creationtime);
        fileToInodeMap.put(filename, new Inode(filename, creationtime));
    }

    /**
     * It returns Immutable view of File to Inode map
     * 
     * @return Collections Immutable Map
     */
    public static Map<String, Inode> getFileToInodeMap() {
        return Collections.unmodifiableMap(fileToInodeMap);
    }

}
