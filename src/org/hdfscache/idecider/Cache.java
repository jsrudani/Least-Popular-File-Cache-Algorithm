package org.hdfscache.idecider;

/**
 * This interface provide basic implementation contract that every type of cache has to implement.
 * @author jrrudani
 *
 */
public interface Cache {

    /**
     * It is used to read the file. It spawns a new timer task to perform actions related to each cache. 
     * For e.g. for Least Popular File it will perform popularity calculations.
     * @param file
     */
    public void read(Inode file);

}
