import java.util.ArrayList;

public class IndexEntry {
    
    private ArrayList<Integer> dstorePorts;
    private IndexEntryStatus status;
    private int fileSize;

    public IndexEntry(int fileSize){
        dstorePorts = new ArrayList<>();
        status = IndexEntryStatus.STORE_IN_PROGRESS;
        this.fileSize = fileSize;
    }

    public IndexEntry(Integer dstorePort, int fileSize){
        dstorePorts = new ArrayList<>();
        dstorePorts.add(dstorePort);
        status = IndexEntryStatus.STORE_IN_PROGRESS;
        this.fileSize = fileSize;
    }

    public void setStatus(IndexEntryStatus newStatus){
        this.status = newStatus;
    }

    public void setStoreComplete(){
        status = IndexEntryStatus.STORE_COMPLETE;
    }

    public void setRemoveInProgress(){
        status = IndexEntryStatus.REMOVE_IN_PROGRESS;
    }

    public void setRemoveComplete(){
        status = IndexEntryStatus.REMOVE_COMPLETE;
    }

    public boolean isAvailable(){
        return status == IndexEntryStatus.STORE_COMPLETE || status == IndexEntryStatus.REMOVE_COMPLETE;
    }

    public boolean isDeleted(){
        return status == IndexEntryStatus.REMOVE_COMPLETE;
    }

    public void addDstore(Integer dstorePort){
        dstorePorts.add(dstorePort);
    }

    public void removeDstore(Integer dstorePort){
        dstorePorts.remove(dstorePort);
    }

    public boolean isStoreComplete(){
        return status == IndexEntryStatus.STORE_COMPLETE;
    }

    public ArrayList<Integer> getDstorePorts(){
        return dstorePorts;
    }

    public int getFileSize(){
        return fileSize;
    }
    
}

enum IndexEntryStatus {
    STORE_IN_PROGRESS,
    STORE_COMPLETE,
    REMOVE_IN_PROGRESS,
    REMOVE_COMPLETE
}
