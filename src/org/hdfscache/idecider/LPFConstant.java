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
    /**
     * It represents Total number of files can be cache. So total 3 files can be
     * cached.
     */
    public static final long TOTAL_CACHE_ENTRY = 4L;
    /**
     * It represents the caching eligiblity for file. If file access count is
     * greater than threshold then it is eligible for caching.
     */
    public static final long LPF_ACCESS_COUNT_THRESHOLD = 1L;
    /**
     * It represents Thread pool size used to execute the task submitted to
     * executor.
     */
    public static final int THREAD_POOL_SIZE = 6;
    /**
     * It represents threshold for window size. If the window size increase
     * above threshold value then set the window size to threshold value. The
     * value of threshold is configured based on application.
     */
    public static final long WINDOW_SIZE_THRESHOLD = 25L;

}
