package org.mariadb.dyncol.blob;

/*
 MariaDB Dynamic column java plugin
 Copyright (c) 2016 MariaDB.
 ...
 ...
 */

/** This class contain information about blob. */
public class BlobDescription {
    /** number of bytes that you need to save size of offset. */
    private int offsetSize;
    /** size of the pool of names in bytes. */
    private int nmpoolSize = 0;
    /** size of the Blob in bytes. */
    private int blobSize = 0;
    /** numer of the byte after header part. */
    private int headerOffset = 0;

    public void setOffsetSize(int offsetSize) {
        this.offsetSize = offsetSize;
    }

    public void setNmpoolSize(int nmpoolSize) {
        this.nmpoolSize = nmpoolSize;
    }

    public void setBlobSize(int blobSize) {
        this.blobSize = blobSize;
    }

    public void setHeaderOffset(int headerOffset) {
        this.headerOffset = headerOffset;
    }

    public int getOffsetSize() {
        return offsetSize;
    }

    public int getNmpoolSize() {
        return nmpoolSize;
    }

    public int getBlobSize() {
        return blobSize;
    }

    public int getHeaderOffset() {
        return headerOffset;
    }
}
