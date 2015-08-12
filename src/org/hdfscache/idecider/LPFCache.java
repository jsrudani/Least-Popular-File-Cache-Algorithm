package org.hdfscache.idecider;

/**
 * This class implements LPF cache.
 * @author jrrudani
 *
 */
public class LPFCache implements Cache {

    @Override
    public void read(Inode file) {
        synchronized (file) {
            // Increment the access count
            file.incrementAndSetAccesscount();
        }
    }
}
