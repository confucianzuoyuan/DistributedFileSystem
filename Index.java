import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

public class Index {
    
    private ConcurrentHashMap<String,IndexEntry> index;
    private Thread rebalanceThread;

    public Index(Thread rebalanceThread){
        index = new ConcurrentHashMap<>();
        this.rebalanceThread = rebalanceThread;
    }

    synchronized public Boolean addNewEntry(String fileName, int fileSize){
        if (!index.containsKey(fileName)){
            System.out.println("INDEX: Adding file: " + fileName);
            index.put(fileName, new IndexEntry(fileSize));
            return true;
        } else return false;
    }

    public void addDstoreToFile(String fileName, Integer dstorePort){
        if (index.containsKey(fileName))
        index.get(fileName).addDstore(dstorePort);
    }

    public ArrayList<String> getReadyFilenames(){

        ArrayList<String> fileNames = new ArrayList<>();

        for (Map.Entry<String,IndexEntry> e : index.entrySet()) {
            if (e.getValue().isAvailable()){
                fileNames.add(e.getKey());
            }
        }

        return fileNames;
    }

    public void completeStore(String fileName){
        index.get(fileName).setStoreComplete();

        if (!inProgressTransation() && rebalanceThread != null){
            // System.out.print("INDEX: Store done, notifying rebalance thread");
            rebalanceThread.notify();
        }
    }

    public void completeRemove(String fileName){
        index.get(fileName).setRemoveComplete();

        if (!inProgressTransation() && rebalanceThread != null){
            // System.out.print("INDEX: Remove done, notifying rebalance thread");
            rebalanceThread.notify();
        }
    }

    public void removeIndexEntry(String fileName){
        index.remove(fileName);
    }

    public IndexEntry getEntry(String fileName){
        return index.get(fileName);
    }

    public KeySetView<String, IndexEntry> getFiles(){
        return index.keySet();
    }
    
    public boolean inProgressTransation(){
        for (IndexEntry entry : index.values()){
            if (!entry.isAvailable()) return true;
        }
        return false;
    }
}
