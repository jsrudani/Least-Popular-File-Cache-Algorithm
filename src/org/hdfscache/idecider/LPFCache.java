package org.hdfscache.idecider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class implements LPF cache.
 * 
 * @author jrrudani
 * 
 */
public class LPFCache implements Cache {
    /**
     * This indicates the number of files already in cache
     */
    private volatile AtomicLong currentFileCacheCount = new AtomicLong(1);
    /**
     * Executor which is used to submit the cache and uncache task
     */
    private final ExecutorService cacheUncacheTaskExecutor = Executors.newFixedThreadPool(LPFConstant.THREAD_POOL_SIZE);

    @Override
    public void read(Inode file) {
        synchronized (file) {
            // Increment the access count
            file.incrementAndSetAccesscount();
            // Check if file is already cached or not. If yes then hit else miss
            if (file.isCached()) {
                // Hit
            } else {
                // Miss
                // Eligiblity check

            }
        }
    }

    /**
     * This method is used to check if file is eligible for caching. It compares
     * the File access count with threshold value which is configurable. If file
     * is eligible then it checks whether cache is full or not. If cache is full
     * then spawns thread to remove least popular file and add current file else
     * perform normal thread to cache the current file. So if you find that this
     * algorithm is busy in "Thrashing" then it might be because of window size
     * or cache size is not properly configured.
     * 
     * @param file
     */
    private void performCacheOperation(Inode file) {
        if (file.getAccesscount() > LPFConstant.LPF_ACCESS_COUNT_THRESHOLD) {
            // Check if cache is full or not. As in the trace file, size is not
            // mention. So here we are using total cache entry. If we know the
            // size of each file then we can compare the required size with
            // current cache size.
            if (currentFileCacheCount.longValue() < LPFConstant.TOTAL_CACHE_ENTRY) {
                cacheUncacheTaskExecutor.submit(new AddToCache(file));
            } else {
                cacheUncacheTaskExecutor.submit(new MakeRoomNAddToCache(file));
                currentFileCacheCount.decrementAndGet();
            }
            file.setCached(true);
            currentFileCacheCount.incrementAndGet();
        }
    }

    /**
     * It calculates popularity of file based on its access count/age and other
     * file characteristics.
     * 
     * @param file
     */
    private void calculatePopularity(Inode file) {

    }

    /**
     * It is used to add it to LPF cache. LPF cache is sorted based on file
     * popularity. Whenever new file is added to LPF cache, a worker thread is
     * created to check whether file is still eligible to be in cache. Worker
     * thread runs at every window expiration for each file.
     * 
     * @param file
     */
    private void addToLPFCache(Inode file) {

    }

    /**
     * This class is used to add file to cache.It implements runnable as
     * seperate thread is used to run this task.
     * 
     * @author jsrudani
     * 
     */
    class AddToCache implements Runnable {

        private final Inode file;

        AddToCache(Inode file) {
            this.file = file;
        }

        @Override
        public void run() {
            // Cal. Popularity
            calculatePopularity(file);
            // Add to LPF Cache
            addToLPFCache(file);
        }
    }

    /**
     * This class is used to remove the least popular file and add new file to
     * cache.
     * 
     * @author jsrudani
     * 
     */
    class MakeRoomNAddToCache implements Runnable {

        private final Inode file;

        MakeRoomNAddToCache(Inode file) {
            this.file = file;
        }

        @Override
        public void run() {

        }
    }
}
