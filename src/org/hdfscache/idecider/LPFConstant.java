package org.hdfscache.idecider;

/**
 * This class holds all the constants value
 * 
 * @author jsrudani
 * 
 */
public class LPFConstant {
    /**
     * It represents File open operation
     */
    public static final String FILE_OPEN = "open";
    /**
     * It represents File close operation
     */
    public static final String FILE_CLOSE = "close";
    /**
     * It represents File delete operation
     */
    public static final String FILE_DELETE = "delete";
    /**
     * It represents File create operation
     */
    public static final String FILE_CREATE = "create";
    /**
     * It represents Default Window size for File
     */
    public static final long DEFAULT_WINDOW_SIZE = 10L;
    /**
     * It represents Least Frequently Used Cache
     */
    public static final String LFU_CACHE_TYPE = "LFU";
    /**
     * It represents Least Recently Used Cache
     */
    public static final String LRU_CACHE_TYPE = "LRU";
    /**
     * It represents Least Popular File Cache
     */
    public static final String LPF_CACHE_TYPE = "LPF";

}
