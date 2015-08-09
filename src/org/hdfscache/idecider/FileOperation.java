package org.hdfscache.idecider;

/**
 * This class represents file operation like open/close/create/delete
 * 
 * @author jsrudani
 * 
 */
public class FileOperation {

    /**
     * It performs file read operation for given filename
     * 
     * @param filename
     *            The file to be read
     * @throws IllegalArgumentException
     */
    public static void open(String filename)
            throws IllegalArgumentException {
        System.out.println("Open " + filename + " at " + System.currentTimeMillis());
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
        System.out.println("Create " + filename + " at " + System.currentTimeMillis());
    }

}
