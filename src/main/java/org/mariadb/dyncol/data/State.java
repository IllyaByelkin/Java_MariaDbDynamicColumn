package org.mariadb.dyncol.data;

import java.util.HashMap;
import java.util.Map;

import org.mariadb.dyncol.blob.BlobDescription;

/** This class contain HashMap with data, format of the DynamiColumns and description of the blob*/
public class State {
    private Map<String, Member> data = new HashMap<>();
    private boolean columnsWithStringFormat = false;
    BlobDescription blbDescription;

    public Map<String, Member> getData() {
        return data;
    }

    public void setData(Map<String, Member> data) {
        this.data = data;
    }

    public boolean isColumnsWithStringFormat() {
        return columnsWithStringFormat;
    }

    public void setColumnsFormat(boolean columnsWithStringFormat) {
        this.columnsWithStringFormat = columnsWithStringFormat;
    }
    
    public void createBlobDescription() {
        blbDescription = new BlobDescription();
    }
    
    public BlobDescription getBlobDescription() {
        return blbDescription;
    }
}
