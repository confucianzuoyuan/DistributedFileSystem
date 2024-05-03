import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class ControllerClientHandler extends Thread {
    private Socket clientSocket;
    private Controller controller;
    private PrintWriter out;
    private BufferedReader in;

    int dstorePort;

    int dstoreIndex;
    String inputLine;

    TimerTask task;
    Timer timer;

    private boolean closed;

    public ControllerClientHandler(Socket clientSocket, Controller controller){
        // System.out.println("CONTROLLER SYSTEM: Starting client socket");
        this.clientSocket = clientSocket;
        this.controller = controller;
        closed = false;
        dstorePort = 0;
        dstoreIndex = 0;
    }

    public void run(){
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            currentThread().setName("client" + controller.getClientCount());

            while(!closed){

                if ((inputLine = in.readLine()) != null){
                    System.out.println(currentThread().getName() + "-CONTROLLER SYSTEM: RECEIEVED = " + inputLine);
                    interpretInput(inputLine);
                }
            }
            
            System.out.println("CONTROLLER SYSTEM: CLOSING");

            in.close();
            out.close();
            clientSocket.close();
            if (dstorePort != 0){
                controller.removeDstore(dstorePort);
            }
            this.interrupt();

        } catch (IOException e) {
            if (dstorePort != 0){
                controller.removeDstore(dstorePort);
            } else e.printStackTrace();
        }
    
    }

    private void interpretInput(String input){
        
            String[] words = input.split(" ");

            if (words[0].equals("STORE") && words.length == 3){
                if (controller.enoughDstores()){
                    handleStoreOperation(words);
                } else out.println("ERROR_NOT_ENOUGH_DSTORES");

            } else if (words[0].equals("LOAD") && words.length == 2){
                if (controller.enoughDstores()){
                    dstoreIndex = 0;
                    handleLoadOperation(words);
                } else out.println("ERROR_NOT_ENOUGH_DSTORES");

            } else if (words[0].equals("RELOAD") && words.length == 2){
                dstoreIndex++;
                handleLoadOperation(words);

            } else if (words[0].equals("JOIN") && words.length == 2){
                dstorePort = Integer.parseInt(words[1]);
                currentThread().setName("dstore"+dstorePort);
                controller.addDstore(dstorePort, this);

            } else if (words[0].equals("STORE_ACK") && words.length == 2){
                String fileName = words[1];
                controller.dstoreAck(dstorePort, fileName);

            } else if (words[0].equals("REMOVE") && words.length == 2){
                if (controller.enoughDstores()){
                    handleRemoveOperation(words);
                } else out.println("ERROR_NOT_ENOUGH_DSTORES");

            } else if (words[0].equals("REMOVE_ACK") && words.length == 2){
                String fileName = words[1];
                controller.removeAck(dstorePort, fileName);

            } else if (input.equals("LIST") && dstorePort == 0){
                out.println("LIST" + controller.getFileList());

            } else if (words[0].equals("LIST") && dstorePort != 0){
                receiveFileList(words);
                inputLine = "";
            } else {
                //Handle invalid request?
            }
        inputLine = "";
    }

    private void handleStoreOperation(String[] words){

        if (controller.firstRebalance && controller.getRebalanceThread() != null){
            do {
                    if (controller.getRebalanceThread().isAlive()){
                        try {
                            System.out.println(currentThread().getName() + "-CONTROLLER: WAITING FOR REBALANCE TO FINISH");
                            sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
            } while (controller.getRebalanceThread().isAlive());
        }

        String fileName = words[1];
        int fileSize = Integer.parseInt(words[2]);
            
        if (!controller.addNewFile(fileName, fileSize)){
            System.out.println(currentThread().getName() + "-FILE ALREADY EXISTS " + fileName);
            out.println("ERROR_FILE_ALREADY_EXISTS");
            
        } else {
            ArrayList<Integer> DstorePorts = controller.handleStoreRequest(fileName, this);
            
            String response = "STORE_TO";
            for (Integer port : DstorePorts){
                response = response + " " + port;
            }
    
            ControllerClientHandler thisHandler = this;
            boolean storeComplete = false;

            synchronized (this){
                try {
                    out.println(response);

                    while (!storeComplete){
                        task = new TimerTask() {
                            public void run(){
                                System.out.println(currentThread().getName() + "-STORE TIMEOUT");
                                controller.deleteFileIndex(fileName, thisHandler);
                            }
                        };
                        timer = new Timer();
                        timer.schedule(task, (long) controller.timeout);
                        wait();
                        task.cancel();
                        timer.cancel();
                        storeComplete = controller.checkStoreComplete(fileName);
                        System.out.println(currentThread().getName() + "-STORE COMPLETE: " + storeComplete);
                    }
                    
                    out.println("STORE_COMPLETE");
                    System.out.println(currentThread().getName() + "-sent store complete");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private void handleLoadOperation(String[] words){
        int port = controller.getDstoreStroringFile(words[1],dstoreIndex);
        System.out.println("LOAD OPERATION, Port: " + port);

        if (port == -1){
            // System.out.print(currentThread().getName() + "-SENDING: " + "ERROR_FILE_DOES_NOT_EXIST");
            out.println("ERROR_FILE_DOES_NOT_EXIST");
        } else if (port == -2){
            // System.out.print(currentThread().getName() + "-SENDING: " + "ERROR_LOAD");
            out.println("ERROR_LOAD");
        } else {
            // System.out.print(currentThread().getName() + "-SENDING: " + "LOAD_FROM " + port + " " + controller.getFileSize(words[1]));
            out.println("LOAD_FROM " + port + " " + controller.getFileSize(words[1]));
        }

    }

    private void handleRemoveOperation(String[] words){
        if (controller.firstRebalance && controller.getRebalanceThread() != null){
            do {
                    if (controller.getRebalanceThread().isAlive()){
                        try {
                            System.out.println(currentThread().getName() + "-CONTROLLER: WAITING FOR REBALANCE TO FINISH");
                            sleep(100);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
            } while (controller.getRebalanceThread().isAlive());
        }

        String fileName = words[1];
            
                ControllerClientHandler thisHandler = this;
                if (controller.canRemoveFile(fileName, this)){
                    try {
                        synchronized (this){

                            ArrayList<Integer> ports = controller.getIndexEntry(fileName).getDstorePorts();

                            for (int i = 0; i < ports.size(); i++){
                                task = new TimerTask() {
                                    public void run(){
                                        System.out.println(currentThread().getName() + "-REMOVE TIMEOUT");
                                        controller.deleteFileIndex(fileName, thisHandler);
                                        return;
                                    }
                                };
                                timer = new Timer();
                                timer.schedule(task, (long) controller.timeout);
                                controller.sendRemoveToDstore(ports.get(i), fileName);
                                wait();
                                task.cancel();
                                timer.cancel();
                            }


                            out.println("REMOVE_COMPLETE");
                            controller.removeComplete(fileName);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            } else {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
            }

    }

    synchronized public void sendRemoveToDstore(String fileName){
        // System.out.println("CONTROLLER: SENDING REMOVE TO DSTORE " + fileName);
        out.println("REMOVE " + fileName);
    }

    synchronized public void sendRebalanceMessage(String message){
        out.println(message);
    }

    synchronized public void sendListMessageToDstore(){
        if (dstorePort > 0){
            out.println("LIST");
        }
    }

    private void receiveFileList(String[] words){
        ArrayList<String> fileList = new ArrayList<>();
                
        if (words[0].equals("LIST")){
            for (int i = 1; i < words.length; i++){
                // System.out.print("CONTROLLER: Dstore " + dstorePort + " has file: " + words[i]);
                fileList.add(words[i]);
            }
        }
        controller.receiveFileList(dstorePort, fileList);
    }
}
