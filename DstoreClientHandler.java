import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

public class DstoreClientHandler extends Thread {
    private Socket clientSocket;
    private Dstore dstore;
    private PrintWriter out;
    private BufferedReader in;

    private boolean closed;

    private Timer timer;
    private TimerTask task;

    public DstoreClientHandler(Socket clientSocket, Dstore dstore){
        // System.out.println("DSTORE SYSTEM: Starting client socket");
        this.clientSocket = clientSocket;
        this.dstore = dstore;
        closed = false;
    }

    public void run(){
        try {
            while(!closed){

                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String inputLine;

                if ((inputLine = in.readLine()) != null){
                    System.out.println("DSTORE SYSTEM: RECEIEVED = " + inputLine);
                    interpretInput(inputLine);
                }
            }
            
            // System.out.println("DSTORE SYSTEM: CLOSING");

            in.close();
            out.close();
            clientSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    
    }

    private void interpretInput(String input){
        
        String[] words = input.split(" ");

        if (words[0].equals("STORE") && words.length == 3){

            System.out.println("ACK");
            out.println("ACK");

            handleFile(words[1],Integer.parseInt(words[2]));

        } else if (words[0].equals("LOAD_DATA") && words.length == 2){
            // System.out.println("DSTORE SYSTEM: LOAD COMMAND DETECTED ");

            System.out.println("DSTORE SYSTEM: Sending file " + words[1]);
            sendFile(words[1]);
            
            
        }


    }

    private void handleFile(String fileName, int fileSize){
        // System.out.println("FILE HANDLE");
        byte[] data = new byte[fileSize];

        int bytesRead = 0;
        int current = 0;

        File file = new File(dstore.getFolderName() + File.separator + fileName);

        try {
                BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));

                do {
                    bytesRead = clientSocket.getInputStream().read(data,current,fileSize-current);
                    if (bytesRead >= 0){
                        current += bytesRead;
                    }
                    // System.out.println("DSTORE SYSTEM: Downloading file " + bytesRead + "/" + fileSize);
                } while (bytesRead < fileSize);

                outputStream.write(data,0,current);
                outputStream.flush();
                // System.out.println("DSTORE SYSTEM: File  " + fileName + " downloaded");

                dstore.sendStoreAck(fileName);

                outputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendFile(String fileName){
        File file = new File(dstore.getFolderName() + File.separator + fileName);
        int filesize = (int) file.length();
        byte[] data = new byte [filesize];

        if (file.exists()){
            try {

                BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
                input.read(data,0,data.length);
                // System.out.println("DSTORE: Sending file of size " + filesize);
                clientSocket.getOutputStream().write(data,0,filesize);
                clientSocket.getOutputStream().flush();
                System.out.println("DSTORE: File sent");
                input.close();
                
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
