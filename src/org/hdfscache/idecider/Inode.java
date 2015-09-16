package org.hdfscache.idecider;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents File meta-data information. Each file is represent by
 * its Inode.
 * 
 * @author jsrudani
 * 
 */
public class Inode {

    /**
     * It is used to count the number of files. It is incremented atomically for
     * every new file created.
     */
    private static AtomicLong fileCounter = new AtomicLong(0);

    /**
     * It is used to uniquely identifies the file.
     */
    private final long inodeId;
    /**
     * It denotes the path of a file.
     */
    private final String path;
    /**
     * It denotes the number of times file is accessed. The access count is
     * reset at window expiration if file is not popular any further.
     */
    private volatile long accesscount;
    /**
     * It denotes the time span for a file to be in cache. Window size is either
     * incremented/decremented based on popularity value.
     */
    private volatile long windowsize;
    /**
     * It is used to denote if file is cached or not.
     */
    private volatile boolean isCached;
    /**
     * It represents the creation time of a file.
     */
    private long creationtime;
    /**
     * It denotes the access time for a file. It is updated whenever file is
     * read. It is used to determine the last access time based on window
     * timespan.
     */
    private volatile long accesstime;
    /**
     * It denotes the popularity value of a file. The popularity value is
     * calculated based on file access count, file window size and file age
     * (Last time file was accessed in a window).
     */
    private volatile float popularity;
    /**
     * It is used denote the last access to a file in its window. Since it is
     * asynchronous environment so caching and uncaching task is handled by
     * background component. So at window expiration we need to know the last
     * time file was accessed in that particular window and this variable holds
     * that value.
     */
    private volatile long lastAccessTime;
    /**
     * It denotes the start time for a window. It helps to calculate the total
     * timespan for a window. It is used in determining the last access time.
     * Whenever file is uncache or not eligible for caching the value is reset.
     */
    private volatile long startWindowTime;

    Inode(String filename, long createtime) {
        this(fileCounter.longValue(), filename, createtime, LPFConstant.DEFAULT_ACCESS_TIME, LPFConstant.DEFAULT_ACCESS_COUNT, false, LPFConstant.DEFAULT_WINDOW_SIZE, LPFConstant.DEFAULT_POPULARITY_VALUE, LPFConstant.DEFAULT_START_WINDOW_TIME, LPFConstant.DEFAULT_LAST_ACCESS_TIME);
        fileCounter.incrementAndGet();
    }

    Inode(long inodeid, String path, long creationtime,
            long accesstime, long accesscount,
            boolean isCached, long windowsize,
            float popularity, long startWindowTime,
            long lastAccessTime) {
        this.inodeId = inodeid;
        this.path = path;
        this.creationtime = creationtime;
        this.accesstime = accesstime;
        this.accesscount = accesscount;
        this.isCached = isCached;
        this.windowsize = windowsize;
        this.popularity = popularity;
        this.startWindowTime = startWindowTime;
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public String toString() {
        return inodeId + "|" + path + "|" + accesscount + "|" + windowsize + "|" + isCached + "|" + creationtime + "|" + accesstime + "|" + popularity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (inodeId ^ (inodeId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Inode other = (Inode) obj;
        if (inodeId != other.inodeId)
            return false;
        return true;
    }

    public synchronized long getAccesscount() {
        return accesscount;
    }

    public synchronized void incrementAndSetAccesscount() {
        this.accesscount += 1;
    }

    public synchronized void resetFileAccesscount() {
        this.accesscount = 0;
    }

    public long getWindowsize() {
        return windowsize;
    }

    public void setWindowsize(long windowsize) {
        this.windowsize = windowsize;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean isCached) {
        this.isCached = isCached;
    }

    public long getCreationtime() {
        return creationtime;
    }

    public void setCreationtime(long creationtime) {
        this.creationtime = creationtime;
    }

    public long getAccesstime() {
        return accesstime;
    }

    public void setAccesstime(long accesstime) {
        this.accesstime = accesstime;
    }

    public float getPopularity() {
        return popularity;
    }

    public void setPopularity(float popularity) {
        this.popularity = popularity;
    }

    public long getInodeId() {
        return inodeId;
    }

    public String getPath() {
        return path;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getStartWindowTime() {
        return startWindowTime;
    }

    public void setStartWindowTime(long startWindowTime) {
        this.startWindowTime = startWindowTime;
    }

    public synchronized void resetStartWindowTime() {
        this.startWindowTime = 0;
    }

    /**
     * It is used to store last access time for a file. Initially when access
     * time for a file is default value, it checks if access time is default
     * value then set the current time which is passed as an argument else set
     * the previous access time.
     * 
     * @param currentTime
     */
    public void checkAndSetLastAccessTime(long currentTime) {
        if (currentTime < (this.startWindowTime + this.windowsize)) {
            long currentAccessTime = (this.accesstime != LPFConstant.DEFAULT_ACCESS_TIME) ? this.accesstime : currentTime;
            setLastAccessTime(currentAccessTime);
        }
    }

}
