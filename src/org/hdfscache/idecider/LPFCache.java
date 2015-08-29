package org.hdfscache.idecider;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class implements LPF cache.
 * 
 * @author jrrudani
 * 
 */
public class LPFCache implements Cache {
    /**
     * This represents Least Popular File cache. It is thread safe and efficient
     * as it locks certain region of Map not the entire Map. It is sorted based
     * on Popularity value.
     */
    private static volatile ConcurrentSkipListMap<Inode, Long> LPFCACHE = new ConcurrentSkipListMap<Inode, Long>(new Comparator<Inode>() {
        @Override
        public int compare(Inode o1, Inode o2) {
            return (int) (o1.getPopularity() - o2.getPopularity());
        }
    });
    /**
     * This represents the current total number of cached file. Every time file
     * is added to LPF cache, count is incremented atomically.
     */
    private static volatile AtomicLong numberOfCachedFile = new AtomicLong(0);
    /**
     * This represents the total number of cache hit. Every time file is
     * accessed and it is already cached then hit count is incremented
     * atomically.
     */
    private static volatile AtomicLong LPF_CACHE_HIT = new AtomicLong(0);
    /**
     * This represents the total number of cache miss. Every time file is
     * accessed and it is not cached then miss count is incremented atomically.
     */
    private static volatile AtomicLong LPF_CACHE_MISS = new AtomicLong(0);
    /**
     * This represents the total number of request. Every time file is accessed
     * then request count is incremented atomically.
     */
    private static volatile AtomicLong LPF_CACHE_TOTAL_REQUEST = new AtomicLong(0);
    /**
     * This represents collection of all popularity values in ascending order.
     */
    private static volatile ConcurrentSkipListSet<Float> popularityOrderedValueSet = new ConcurrentSkipListSet<Float>();
    /**
     * Executor which is used to submit the cache and uncache task. It is used
     * to spawn new thread to re-calculate the popularity at window expiration.
     * It also judge new window size based on popularity value.
     */
    private final ScheduledExecutorService cacheUncacheTaskExecutor = Executors.newScheduledThreadPool(LPFConstant.THREAD_POOL_SIZE);

    @Override
    public void read(Inode file) {
        synchronized (file) {
            try {
                // Increment the access count
                file.incrementAndSetAccesscount();
                // Set the access time
                file.setAccesstime(System.currentTimeMillis());
                // Increment the total request count
                LPF_CACHE_TOTAL_REQUEST.incrementAndGet();
                // Check if file is already cached or not. If yes then hit else
                // miss
                if (file.isCached()) {
                    // Hit. Log the hit count
                    LPF_CACHE_HIT.incrementAndGet();
                } else {
                    // Miss. Log the miss count
                    LPF_CACHE_MISS.incrementAndGet();
                    // Perform caching
                    performCacheOperation(file);
                }
                /*
                 * System.out.println("Statistics after reading file " +
                 * file.getInodeId());
                 * System.out.println("LPF_CACHE_TOTAL_REQUEST -> " +
                 * LPF_CACHE_TOTAL_REQUEST.get());
                 * System.out.println("LPF_CACHE_HIT -> " +
                 * LPF_CACHE_HIT.get()); System.out.println("LPF_CACHE_MISS -> "
                 * + LPF_CACHE_MISS.get());
                 */
            } catch (Exception ex) {
                System.out.println("read - > There is some problem");
                ex.printStackTrace();
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
     * @throws Exception
     */
    private void performCacheOperation(Inode file)
            throws Exception {
        if (file.getAccesscount() > LPFConstant.LPF_ACCESS_COUNT_THRESHOLD) {
            // Check if cache is full or not. As in the trace file, size is not
            // mention. So here we are using total cache entry. If we know the
            // size of each file then we can compare the required size with
            // current cache size.
            if (numberOfCachedFile.longValue() < LPFConstant.TOTAL_CACHE_ENTRY) {
                cacheUncacheTaskExecutor.submit(new AddToCache(file));
            } else {
                cacheUncacheTaskExecutor.submit(new MakeRoomNAddToCache(file));
                numberOfCachedFile.decrementAndGet();
            }
            file.setCached(true);
            numberOfCachedFile.incrementAndGet();
        }
    }

    /**
     * It calculates popularity of file based on its access count/age and other
     * file characteristics. The popularity value is compared with 50% of Least
     * Popular and Most Popular value. Based on comparison window size for file
     * will be increase/decrease.
     * 
     * @param file
     * @throws Exception
     */
    private void calculatePopularity(Inode file)
            throws Exception {
        synchronized (file) {
            float newPopularity = 0.0f;
            long newWindowSize = 0L;
            float thresholdPopularity = 0.0f;
            long index = 0;
            // Calculate the popularity value for a file based on access count
            // and window size
            if (file.getWindowsize() != 0) {
                float fileAccessRate = (((float) file.getAccesscount()) / ((float) file.getWindowsize()));
                long fileAge = Math.abs((System.currentTimeMillis() - file.getAccesstime()));
                newPopularity = ((float) fileAccessRate) / ((float) fileAge);
                // Compare the calculated popularity value with Least Popular
                // and Most Popular value from the LPF cache
                long size = (numberOfCachedFile.get() - 1);
                index = size / 2;
                float firstOperand = getMedianPopularityValue(index);
                if (!popularityOrderedValueSet.isEmpty()) {
                    if (size % 2 == 0) {
                        float secondOperand = popularityOrderedValueSet.higher(firstOperand);
                        thresholdPopularity = (firstOperand + secondOperand) / 2;
                    } else {
                        thresholdPopularity = firstOperand;
                    }
                }
                // Double the window size if new popularity is greater than old
                // popularity else half the window size. Increase the window
                // size upto certain limit.
                if (newPopularity > thresholdPopularity) {
                    newWindowSize = file.getWindowsize() * 2;
                } else {
                    newWindowSize = file.getWindowsize() / 2;
                }
                // Compare with threshold value
                if (newWindowSize > LPFConstant.WINDOW_SIZE_THRESHOLD) {
                    newWindowSize = LPFConstant.WINDOW_SIZE_THRESHOLD;
                }
            }
            // Set the new popularity and window size
            file.setPopularity(newPopularity);
            file.setWindowsize(newWindowSize);
        }
    }

    /**
     * It is used to get the median value out of all sorted popularity value.
     * Currently we are traversing the sorted set and get the median. But in the
     * future we need to make data structure like SortedList so that it is
     * sorted on every insert and also have random access.
     * 
     * @param index
     * @throws Exception
     */
    private float getMedianPopularityValue(long index)
            throws Exception {
        long traverseCount = 0L;
        Iterator<Float> popularityValueIterator = popularityOrderedValueSet.iterator();
        while (popularityValueIterator.hasNext()) {
            if (traverseCount == index) {
                return popularityValueIterator.next();
            }
            traverseCount += 1;
            popularityValueIterator.next();
        }
        return 0;
    }

    /**
     * It is used to add it to LPF cache. LPF cache is sorted based on file
     * popularity. Whenever new file is added to LPF cache, a worker thread is
     * created to check whether file is still eligible to be in cache. Worker
     * thread runs at every window expiration for each file.
     * 
     * @param file
     * @throws Exception
     */
    private void addToLPFCache(Inode file) throws Exception {
        synchronized (file) {
            // Check if file is in cache
            if (!LPFCACHE.containsKey(file)) {
                // Insert into Sorted set the priority value
                popularityOrderedValueSet.add(file.getPopularity());
                LPFCACHE.put(file, file.getInodeId());
                file.setCached(true);
                // spawn new thread to check for window expiration
                cacheUncacheTaskExecutor.schedule(new CacheUncacheTask(file), file.getWindowsize(), TimeUnit.MILLISECONDS);
            }
            System.out.println("LPF Cache for adding file -> " + file.getInodeId() + " -> " + LPFCACHE);
        }
    }

    public static AtomicLong getLPF_CACHE_HIT() {
        return LPF_CACHE_HIT;
    }

    public static AtomicLong getLPF_CACHE_MISS() {
        return LPF_CACHE_MISS;
    }

    public static AtomicLong getLPF_CACHE_TOTAL_REQUEST() {
        return LPF_CACHE_TOTAL_REQUEST;
    }

    /**
     * This class is used to add file to cache. Seperate thread is used to run
     * this task.
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
            try {
                // Calculate Popularity
                calculatePopularity(file);
                // Add to LPF Cache
                addToLPFCache(file);
            } catch (Exception ex) {
                System.out.println("AddToCache -> Error processing file " + file.getInodeId());
                ex.printStackTrace();
            }
        }
    }

    /**
     * This class is used to remove the least popular file and add new file to
     * cache.Seperate thread is used to run this task.
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
            try {
                // Check if cache is full or not. If not calculate popularity
                // and insert into cache else remove least popular file and then
                // add it to cache
                if (numberOfCachedFile.get() > LPFConstant.TOTAL_CACHE_ENTRY) {
                    // Reset Access count and Window size, clear cache flag for
                    // file which is removed from cache
                    Inode leastPopularFile = LPFCACHE.firstEntry().getKey();
                    leastPopularFile.setCached(false);
                    leastPopularFile.setWindowsize(LPFConstant.DEFAULT_WINDOW_SIZE);
                    leastPopularFile.resetFileAccesscount();
                    // Remove the first entry from Map
                    LPFCACHE.pollFirstEntry();
                    // Decrement the number of cache file
                    numberOfCachedFile.decrementAndGet();
                }
                // Calculate Popularity for new file
                calculatePopularity(file);
                // Add new file to LPF Cache
                addToLPFCache(file);
                // Increment number of cache file
                numberOfCachedFile.incrementAndGet();
            } catch (Exception ex) {
                System.out.println("MakeRoomNAddToCache -> Error processing file " + file.getInodeId());
                ex.printStackTrace();
            }
        }
    }

    /**
     * This class performs caching and un-caching task. Seperate thread is
     * running to check if file needs to be in cache. It is activated at window
     * expiration for each file.
     * 
     * @author jsrudani
     * 
     */
    class CacheUncacheTask implements Runnable {
        private final Inode file;

        CacheUncacheTask(Inode file) {
            this.file = file;
        }

        @Override
        public void run() {
            try {
                if (LPFCACHE.containsKey(file)) {
                    // Remove the file from LPF cache
                    if (LPFCACHE.remove(file) != null) {
                        // Decrement count of number of cached file
                        numberOfCachedFile.decrementAndGet();
                        // Calculate Popularity for new file
                        calculatePopularity(file);
                        // Check if file is popular or not
                        if (file.getWindowsize() > 0) {
                            // Reset the file fields
                            file.resetFileAccesscount();
                            // Increment count of number of cached file
                            numberOfCachedFile.incrementAndGet();
                            // Add new file to LPF Cache
                            addToLPFCache(file);
                        } else {
                            // Since new window size is less than expected so
                            // file is removed from the cache
                            file.setCached(false);
                            System.out.println("Not popular at window expiration file -> " + file.getInodeId());
                        }
                    }
                }
            } catch (Exception ex) {
                System.out.println("CacheUncacheTask -> Error processing file " + file.getInodeId());
                ex.printStackTrace();
            }
        }
    }
}
