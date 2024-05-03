import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {
    
    private int cport;
    private int rFactor;
    public double timeout;
    private int rebalancePeriod;

    private ServerSocket serverSocket;
    private ArrayList<ControllerClientHandler> clients;

    private ConcurrentHashMap<Integer, ControllerClientHandler> portToStoreEnd;
    private ConcurrentHashMap<String, StoreRequest> fileNameToReq;

    private ConcurrentHashMap<Integer,ArrayList<String>> currentDstoreFiles;

    private Index index;

    private RebalanceThread rebalanceThread;
    public boolean firstRebalance = false;

    private Timer timer;
    private TimerTask task;

    public static void main(String[] args){
        if (args.length >= 4){
            new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Double.parseDouble(args[2]), Integer.parseInt(args[3]));
        } else System.out.println("Invalid controller arguments");
    }   

    public Controller(int cport, int rFactor, double timeout, int rebalancePeriod){
        this.cport = cport;
        this.rFactor = rFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        clients = new ArrayList<>();
        portToStoreEnd = new ConcurrentHashMap<>();
        fileNameToReq = new ConcurrentHashMap<>(); 
        currentDstoreFiles = new ConcurrentHashMap<>(); 

        resetTimer();

        index = new Index(rebalanceThread);
 
        
            try {
                serverSocket = new ServerSocket(cport);
                // System.out.println("Server socket open: " + !serverSocket.isClosed());
                
                start();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }


    public void start(){
        // System.out.println("Starting");
        while (true){
            try {
                ControllerClientHandler clientHandler = new ControllerClientHandler(serverSocket.accept(), this);
                clients.add(clientHandler);
                clientHandler.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop(){
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void resetTimer(){
        task = new TimerTask() {
            public void run(){
                firstRebalance = true;
                startRebalance();
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, rebalancePeriod * 1000,rebalancePeriod * 1000);
    }


    public void addDstore(int port, ControllerClientHandler endpoint){
        clients.remove(endpoint);
        portToStoreEnd.put(port, endpoint);
        startRebalance();
    }

    public void removeDstore(int port){
        portToStoreEnd.remove(port);
        currentDstoreFiles.remove(port);
    }

    synchronized public boolean addNewFile(String fileName, int fileSize){
        return index.addNewEntry(fileName, fileSize);
    }

    synchronized public ArrayList<Integer> handleStoreRequest(String fileName, ControllerClientHandler clientEndpoint){
        ArrayList<Integer> allocatedDstorePorts = new ArrayList<>();

        try {
            for (Integer i : portToStoreEnd.keySet()){
                allocatedDstorePorts.add(i);
            }

            fileNameToReq.put(fileName, new StoreRequest(clientEndpoint, allocatedDstorePorts));
            return allocatedDstorePorts;

        } catch (Exception e) {
            e.printStackTrace();
        } 
        return allocatedDstorePorts;
    }

    synchronized public boolean canRemoveFile(String fileName, ControllerClientHandler clientEndpoint){
        if (index.getFiles().contains(fileName)){
            IndexEntry entry = index.getEntry(fileName);
            if (entry.isStoreComplete()){
                entry.setRemoveInProgress();
                fileNameToReq.put(fileName, new StoreRequest(clientEndpoint, entry.getDstorePorts()));
                return true;
            } else return false;
        } else return false;
        
    }

    synchronized public void sendRemoveToDstore(int port, String fileName){
        index.getEntry(fileName).setRemoveInProgress();
        portToStoreEnd.get(port).sendRemoveToDstore(fileName);
    }

    public IndexEntry getIndexEntry(String fileName){
        return index.getEntry(fileName);
    }

    synchronized public void deleteFileIndex(String fileName, ControllerClientHandler clientEndpoint){
        if (index.inProgressTransation()){
            index.removeIndexEntry(fileName);
            fileNameToReq.remove(fileName);
        }
    }

    public void removeAck(int port, String fileName){
        if (index.getFiles().contains(fileName)){
            StoreRequest request = fileNameToReq.get(fileName);
            synchronized (request.getClientEndpoint()){
                request.getClientEndpoint().notify();
            }
            System.out.println("Removing dstore store from index");
            index.getEntry(fileName).removeDstore(port);
        }
    }

    synchronized public void removeComplete(String fileName){
            index.completeRemove(fileName);
            fileNameToReq.remove(fileName);
    }

    public void dstoreAck(int port, String fileName){
        if (index.getFiles().contains(fileName) && !index.getEntry(fileName).getDstorePorts().contains(port)){
            StoreRequest storeRequest = fileNameToReq.get(fileName);
            synchronized (storeRequest.getClientEndpoint()){
                storeRequest.getClientEndpoint().notify();
            }
            index.getEntry(fileName).addDstore(port);
        }
    }

    public boolean checkStoreComplete(String fileName){
        // System.out.println("CONTROLLER: Checking if " + fileName + " storage is complete ");
        if (index.getEntry(fileName).getDstorePorts().size() == getDstoreCount()){
            index.completeStore(fileName);
            fileNameToReq.remove(fileName);
            return true;
        } else return false;
    }

    public int getDstoreStroringFile(String fileName, int dstoreIndex){
        IndexEntry entry;
        try {
            entry = index.getEntry(fileName);

            if (dstoreIndex < entry.getDstorePorts().size()){
                return entry.getDstorePorts().get(dstoreIndex);
            } else return -2;
        } catch (Exception e){
                return -1;
        }
    }

    public String getFileList(){
        String list = "";
        for (var f : index.getFiles()){
            list = list + " " + f;
        }
        return list;
    }

    public boolean enoughDstores(){
        return portToStoreEnd.size() >= rFactor;
    }

    public int getFileSize(String fileName){
        return index.getEntry(fileName).getFileSize();
    }

    public int getDstoreCount(){
        return portToStoreEnd.size();
    }

    public int getClientCount(){
        return clients.size();
    }


    // R E B A L A N C E ----------------------------------------------------------------------------------------------------------------------------------

    public Thread getRebalanceThread(){
        return rebalanceThread;
    }

    private void startRebalance(){
        // System.out.println("CONTROLLER: STARTING REBALANCE from thread: " + Thread.currentThread().getName());
        if (firstRebalance){
            if (index.inProgressTransation()){
                try {
                    // System.out.println("REBALANCE: Transaction in progress, waiting...");
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            ArrayList<Integer> dstorePorts = new ArrayList<>();
            if (!portToStoreEnd.isEmpty()){
                timer.cancel();
                dstorePorts.addAll(portToStoreEnd.keySet());

                for (Integer port : dstorePorts){
                    synchronized (portToStoreEnd.get(port)){
                            portToStoreEnd.get(port).sendListMessageToDstore();
                        }
                }
            }
        }
    }

    synchronized void receiveFileList(int port, ArrayList<String> files){
        currentDstoreFiles.put(port, files);
        if (currentDstoreFiles.size() == getDstoreCount()){
            System.out.println("Dstore lists complete...Rebalancing, from port " + port);
            rebalanceThread = new RebalanceThread(this);
            rebalanceThread.start();
        }
    }

    public void rebalance(){
        int minFiles = Math.floorDiv(rFactor, getDstoreCount());
        int maxFiles = (int) Math.ceil((double) rFactor / getDstoreCount());

        // System.out.println("CONTROLLER: REBALANCING, min=" + minFiles + " max=" + maxFiles);

        ArrayList<Integer> dstorePorts = new ArrayList<>();
        dstorePorts.addAll(portToStoreEnd.keySet());

        int dstoreIndex = 0;
        int dstoreCount = getDstoreCount();

        HashMap<String,ArrayList<Integer>> fileNameToNewDstorePort = new HashMap<>();
        HashMap<Integer,ArrayList<String>> dstoreToNewFiles = new HashMap<>();

        //round robin allocation
        // System.out.println("INDEX FILES: ");
        for (String file : index.getFiles()){
            System.out.println(file);
        }


        for (String file : index.getFiles()){

            for (int i = 0; i < rFactor; i++){
                if (dstoreIndex >= dstoreCount){
                    dstoreIndex = 0;
                }
                ArrayList<Integer> newDstores;
                if (fileNameToNewDstorePort.containsKey(file)){
                    newDstores = fileNameToNewDstorePort.get(file);
                } else {
                    newDstores = new ArrayList<>();
                }
                int thisDstore = dstorePorts.get(dstoreIndex);
                newDstores.add(thisDstore);
                fileNameToNewDstorePort.put(file, newDstores);

                ArrayList<String> newFiles;
                if (dstoreToNewFiles.containsKey(thisDstore)){
                    newFiles = dstoreToNewFiles.get(thisDstore);
                } else {
                    newFiles = new ArrayList<>();
                }
                newFiles.add(file);
                dstoreToNewFiles.put(thisDstore, newFiles);
                // System.out.println("DSTORE TO FILE MAP: " + thisDstore + " index: " + dstoreIndex + " new files: ");
                for (String s : newFiles){
                    System.out.println(s);
                }

                dstoreIndex++;
            }
        }

        if (!index.getFiles().isEmpty()){
            for (Integer port : dstorePorts){
                ArrayList<String> filesToRemove = new ArrayList<>();
                HashMap<String, ArrayList<Integer>> filesToSend = new HashMap<>();

                ArrayList<String> currentFiles;
                ArrayList<String> supposedFiles;

                if (dstoreToNewFiles.containsKey(port)){
                    try {
                        supposedFiles = dstoreToNewFiles.get(port);
                    } catch (Exception e) {
                        supposedFiles = new ArrayList<>();
                    }

                    try {
                        currentFiles = currentDstoreFiles.get(port);
                    } catch (Exception e) {
                        currentFiles = new ArrayList<>();
                    }
                } else {
                    supposedFiles = new ArrayList<>();
                    if (currentDstoreFiles.containsKey(port)){
                        currentFiles = currentDstoreFiles.get(port);
                    } else currentFiles = new ArrayList<>();
                }

                //for each current file in this dstore
                for (String currentFile : currentFiles){
                    //it checks all other ports
                    for (Integer otherPort : dstorePorts){
                        if (port == otherPort) break;
                        
                        //to see if it has a file that another dstore needs
                        if (dstoreToNewFiles.containsKey(otherPort)){
                            if (dstoreToNewFiles.get(otherPort).contains(currentFile) && !currentDstoreFiles.get(otherPort).contains(currentFile)){
                                ArrayList<Integer> temp;
                                if (filesToSend.containsKey(currentFile)){
                                    temp = filesToSend.get(currentFile);
                                } else {
                                    temp = new ArrayList<>();
                                }
                                temp.add(otherPort);
                                //adds all files to send and the dstore to send to in a map
                                filesToSend.put(currentFile, temp);

                                //updates current map
                                currentDstoreFiles.get(otherPort).add(currentFile);
                            }
                        }
                    }
                    if (!supposedFiles.contains(currentFile)){
                        filesToRemove.add(currentFile);
                        //and update current map
                        if (dstoreToNewFiles.containsKey(port)){
                            currentDstoreFiles.get(port).remove(currentFile);
                        }
                    }
                }

                //make rebalance message
                String rebalanceMessage = "REBALANCE " + filesToSend.size();

                if (filesToSend.size() > 0){
                    for (Map.Entry<String, ArrayList<Integer>> file : filesToSend.entrySet()){
                        rebalanceMessage = rebalanceMessage + " " + file.getKey() + " " + file.getValue().size();
                        for (Integer dstorePort : file.getValue()){
                            rebalanceMessage = rebalanceMessage + " " + dstorePort;
                        }
                    }
                }

                if (filesToRemove.size() > 0){
                    rebalanceMessage = rebalanceMessage + " " + filesToRemove.size();
                    for (String file : filesToRemove){
                        rebalanceMessage = rebalanceMessage + " " + file;
                    }
                } else {
                    rebalanceMessage = rebalanceMessage + " " + filesToRemove.size();
                }

                //send it
                // System.out.println("Store " + port + " message = " + rebalanceMessage);
                synchronized (portToStoreEnd.get(port)){
                    portToStoreEnd.get(port).sendRebalanceMessage(rebalanceMessage);
                }
                
            }
    }
        currentDstoreFiles.clear();
        resetTimer();
    }


}
