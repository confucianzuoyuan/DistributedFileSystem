import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Dstore {

    private int port;
    private int cport;
    public double timeout;
    private String folderName;

    private ServerSocket clientServerSocket;
    private Socket controllerSocket;

    private boolean receivingClosed;

    private PrintWriter out;
    private BufferedReader in;

    public static void main(String[] args){
        if (args.length >= 4){
            new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Double.parseDouble(args[2]), args[3]);
        } System.out.println("Insufficient dstore arguments");
    }

    public Dstore(int port, int cport, double timeout, String foldername){
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.folderName = foldername;

        receivingClosed = false;
        initFolder();

        try {
            clientServerSocket = new ServerSocket(port);
            // System.out.println("Server socket open: " + !clientServerSocket.isClosed());

            controllerSocket = new Socket(InetAddress.getLoopbackAddress(), this.cport);

            this.out = new PrintWriter(this.controllerSocket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

            this.out.println("JOIN" + " " + this.port);
            
            startClientServerSocket();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void startClientServerSocket(){
        // System.out.println("Starting");
        Dstore dstore = this;

        Thread acceptingThread = new Thread(){
            public void run(){
                while (true){
                    try {
                        DstoreClientHandler dstoreClientHandler = new DstoreClientHandler(clientServerSocket.accept(), dstore);
                        dstoreClientHandler.start();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        acceptingThread.start();;

        while (true){
            try {
                while(!receivingClosed){

                    out = new PrintWriter(controllerSocket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
    
                    String inputLine;
    
                    if ((inputLine = in.readLine()) != null){
                        // System.out.println("DSTORE SYSTEM: RECEIEVED = " + inputLine);
                        interpretInput(inputLine);
                    }
                }
                
                // System.out.println("DSTORE SYSTEM: CLOSING");
    
                in.close();
                out.close();
                controllerSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void interpretInput(String input){
        String[] words = input.split(" ");

        if (words[0].equals("REMOVE") && words.length == 2){
            removeFile(words[1]);
        } else if (words[0].equals("REBALANCE")){
            handleRebalance(input);
        } else if (input.equals("LIST")){
            String response = "LIST";
            for (String file : getFileList()){
                response = response + " " + file;
            }
            // System.out.println("DSTORE " + port + " list: " + response);
            out.println(response);
        }
            
    }

    public void sendStoreAck(String fileName){
        System.out.println("STORE ACK");
        this.out.println("STORE_ACK " + fileName);
    }

    public String getFolderName(){
        return folderName;
    }
    
    private ArrayList<String> getFileList(){
        ArrayList<String> fileList = new ArrayList<>();
        
        File folderPath = new File(folderName);

        if (folderPath.isDirectory()){
            for (File f : folderPath.listFiles()){
                fileList.add(f.getName());
            }
        }
        return fileList;
    }

    private void removeFile(String fileName){
        // System.out.println("DSTORE " +  port + ": REMOVING " + fileName);
        File file = new File(folderName + File.separator + fileName);
        if (file.exists()){
            file.delete();
            out.println("REMOVE_ACK " + fileName);
        } else {
            out.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
        }
    }

    private void initFolder(){
        File folderPath = new File(folderName);

        if (!folderPath.isDirectory()){
            folderPath.mkdirs();
        } else {
            for (File f : folderPath.listFiles()){
                f.delete();
            }
        }
    }

    private void handleRebalance(String message){
        String[] words = message.split(" ");

        int sendFileCount = Integer.parseInt(words[1]);
        int messageIndex = 1;

        if (sendFileCount > 0){
            for (int i = 0; i < sendFileCount; i++){
                messageIndex++;
                String fileName = words[messageIndex];
                messageIndex++;
                int dstoreCount = Integer.parseInt(words[messageIndex]);

                File file = new File(folderName + File.separator + fileName);
                int filesize = (int) file.length();
                byte[] data = new byte [filesize];

                if (file.exists()){
                    for (int j = 0; j < dstoreCount; j++){
                        messageIndex++;
                        int dstorePort = Integer.parseInt(words[messageIndex]);
    
                        Thread sendToOtherDstore = new Thread(){
                            public void run(){
                                try {
                                    Socket dsocket;
                                    dsocket = new Socket(InetAddress.getLoopbackAddress(), dstorePort);
                                
                                    PrintWriter out2 = new PrintWriter(dsocket.getOutputStream(), true);
                                    BufferedReader in2 = new BufferedReader(new InputStreamReader(dsocket.getInputStream()));
                                    out2.println("REBALANCE_STORE " + fileName + " " + filesize);
    
                                    String line2 = in2.readLine();
                                    // System.out.println("SYSTEM: CLIENT RECEIVED " + line2);
    
                                    if (line2.equals("ACK")){
                                        BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
                                        input.read(data,0,filesize);
                                        // System.out.println("DSTORE: Sending file of size " + filesize);
                                        dsocket.getOutputStream().write(data,0,filesize);
                                        dsocket.getOutputStream().flush();
                                        // System.out.println("DSTORE: File sent");
                                        input.close();
                                    }
                                    out2.close();
                                    in2.close();
                                    dsocket.close();
                                    
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        };
                        sendToOtherDstore.start();
                    }
                }
            }
        }

        messageIndex++;
        int removeFileCount = Integer.parseInt(words[messageIndex]);

        if (removeFileCount > 0){
            for (int i = 0; i < sendFileCount; i++){
                messageIndex++;
                File file = new File(folderName + File.separator + words[messageIndex]);
                if (file.exists()){
                    file.delete();
                }
            }
        }

        out.println("REBALANCE_COMPLETE");
    }

}
