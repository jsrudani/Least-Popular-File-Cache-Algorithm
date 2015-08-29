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

    private static AtomicLong fileCounter = new AtomicLong(0);

    private final long inodeId;
    private final String path;
    private volatile long accesscount;
    private volatile long windowsize;
    private volatile boolean isCached;
    private long creationtime;
    private volatile long accesstime;
    private volatile float popularity;

    Inode(String filename, long createtime) {
        this(fileCounter.longValue(), filename, createtime, 0L, 0L, false, LPFConstant.DEFAULT_WINDOW_SIZE, 0.0f);
        fileCounter.incrementAndGet();
    }

    Inode(long inodeid, String path, long creationtime,
            long accesstime, long accesscount,
            boolean isCached, long windowsize,
            float popularity) {
        this.inodeId = inodeid;
        this.path = path;
        this.creationtime = creationtime;
        this.accesstime = accesstime;
        this.accesscount = accesscount;
        this.isCached = isCached;
        this.windowsize = windowsize;
        this.popularity = popularity;
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

}
