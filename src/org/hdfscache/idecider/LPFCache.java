package org.hdfscache.idecider;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
     * This represents Least Popular File cache
     */
    private static ConcurrentSkipListMap<Inode, Long> LPFCACHE = new ConcurrentSkipListMap<Inode, Long>(new Comparator<Inode>() {
        @Override
        public int compare(Inode o1, Inode o2) {
            return (int) (o1.getPopularity() - o2.getPopularity());
        }
    });
    /**
     * This represents the current total number of cached file. Every time file
     * is added to LPF cache, count is incremented
     */
    private static volatile AtomicLong numberOfCachedFile = new AtomicLong(0);
    /**
     * This represents collection of all popularity values in ascending order
     */
    private static ConcurrentSkipListSet<Float> popularityOrderedValueSet = new ConcurrentSkipListSet<Float>();
    /**
     * Executor which is used to submit the cache and uncache task
     */
    private final ExecutorService cacheUncacheTaskExecutor = Executors.newFixedThreadPool(LPFConstant.THREAD_POOL_SIZE);

    @Override
    public void read(Inode file) {
        synchronized (file) {
            // Increment the access count
            file.incrementAndSetAccesscount();
            // Set the access time
            file.setAccesstime(System.currentTimeMillis());
            // Check if file is already cached or not. If yes then hit else miss
            if (file.isCached()) {
                // Hit. Log the hit count
            } else {
                // Miss. Log the miss count
                // performCacheOperation(file);

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
     */
    private void calculatePopularity(Inode file) {
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
            }
            // Compare the calculated popularity value with Least Popular and
            // Most Popular value from the LPF cache
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
            // popularity else half the window size. Increase the window size
            // upto certain limit.
            if (newPopularity > thresholdPopularity) {
                newWindowSize = newWindowSize * 2;
            } else {
                newWindowSize = newWindowSize / 2;
            }
            // Compare with threshold value
            if (newWindowSize > LPFConstant.WINDOW_SIZE_THRESHOLD) {
                newWindowSize = LPFConstant.WINDOW_SIZE_THRESHOLD;
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
     */
    private float getMedianPopularityValue(long index) {
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
     */
    private void addToLPFCache(Inode file) {
        // Check if file is in cache
        if (!LPFCACHE.containsKey(file)) {
            // Insert into Sorted set the priority value
            popularityOrderedValueSet.add(file.getPopularity());
            LPFCACHE.put(file, file.getInodeId());
            file.setCached(true);
            // spawn new thread to check for window expiration
        }
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
            // Calculate Popularity
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
            // Reset Access count and Window size, clear cache flag for file
            // which is removed from cache
            Inode leastPopularFile = LPFCACHE.firstEntry().getKey();
            leastPopularFile.setCached(false);
            leastPopularFile.setWindowsize(LPFConstant.DEFAULT_WINDOW_SIZE);
            leastPopularFile.resetFileAccesscount();
            // Remove the first entry from Map
            LPFCACHE.pollFirstEntry();
            // Calculate Popularity for new file
            calculatePopularity(file);
            // Add new file to LPF Cache
            addToLPFCache(file);
        }
    }
}
