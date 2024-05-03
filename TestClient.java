import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestClient {
    
    private int cport;
    private double timeout;

    private PrintWriter out;
    private BufferedReader in;
    private Socket socket;

    public static void main(String[] args){
        if (args.length == 2){
            new TestClient(Integer.parseInt(args[0]), Double.parseDouble(args[1]));
        }
    }

    public TestClient(int cport, double timeout){
        this.cport = cport;
        this.timeout = timeout;

        try {
            File file = new File("/home/ben/Documents/DistributedFileSystem/soton.jpg");
            int filesize = (int) file.length();
            byte[] data = new byte [filesize];

            socket = new Socket(InetAddress.getLoopbackAddress(), this.cport);
            this.out = new PrintWriter(this.socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            testStore(filesize, data, file, "soton.jpg");
            

            synchronized(this){
                wait(5000);
            }
            this.out.println("REMOVE soton.jpg");
            String line = in.readLine();
            System.out.println("SYSTEM: YO CLIENT RECEIVED " + line);
            // testStore(filesize, data, file, "file2");

            // testStore(filesize, data, file, "file2");

            // out.println("LIST");

            // String line = in.readLine();
            // System.out.println("SYSTEM: CLIENT RECEIVED " + line);

            socket.close();
            // Thread.currentThread().interrupt();
            // while ((line2 = in.readLine()) != null){
            //     System.out.println("SYSTEM: CLIENT RECEIVED " + line2);
            // }

        } catch (Exception e) {
            e.printStackTrace();
            //TODO: handle exception
        }
    }

    private void testStore(int filesize, byte[] data, File file, String fileName){
        try{
            // this.out.println("STORE fileName1 " + filesize);
            this.out.println("STORE " + fileName + " " + filesize);
            System.out.println("SYSTEM: is connected?" + socket.isConnected());
            
            String line = in.readLine();
            System.out.println("SYSTEM: CLIENT RECEIVED " + line);

            String[] words = line.split(" ");
            
            if (words[0].equals("STORE_TO")){
                ArrayList<Integer> ports = new ArrayList<>();
                
                for (int i = 1; i < words.length; i++){
                    ports.add(Integer.parseInt(words[i]));
                }

                for (Integer port : ports){
                    
                    Thread dstoreThread = new Thread(){
                        public void run(){
                            System.out.print("TESTCLIENT: Connecting to dstore with port: " + port);
                            
                            try {
                                Socket dsocket;
                                dsocket = new Socket(InetAddress.getLoopbackAddress(), port);
                            
                                PrintWriter out2 = new PrintWriter(dsocket.getOutputStream(), true);
                                BufferedReader in2 = new BufferedReader(new InputStreamReader(dsocket.getInputStream()));
                                out2.println("STORE " + fileName + " " + filesize);

                                String line2 = in2.readLine();
                                System.out.println("SYSTEM: CLIENT RECEIVED " + line2);

                                if (line2.equals("ACK")){
                                    BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
                                    input.read(data,0,data.length);
                                    System.out.println("TESTCLIENT: Sending file of size " + filesize);
                                    dsocket.getOutputStream().write(data,0,filesize);
                                    dsocket.getOutputStream().flush();
                                    System.out.println("TESTCLIENT: File sent");
                                    input.close();
                                }
                                out2.close();
                                in2.close();
                                dsocket.close();
                                
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    };
                    dstoreThread.start();
                    
                }
                
            }
            
            // this.cport = 1234;
            // socket = new Socket(InetAddress.getLoopbackAddress(), this.cport);
            // this.out = new PrintWriter(this.socket.getOutputStream(), true);
            // this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            // this.out.println("STORE fileName1 " + filesize);

            // String line2 = in.readLine();
            // System.out.println("SYSTEM: CLIENT RECEIVED " + line2);

            // BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
            // input.read(data,0,data.length);
            // System.out.println("TESTCLIENT: Sending file of size " + filesize);
            // socket.getOutputStream().write(data,0,filesize);
            // socket.getOutputStream().flush();
            // System.out.println("TESTCLIENT: File sent");

            // this.cport = 1234;
            // socket = new Socket(InetAddress.getLoopbackAddress(), this.cport);
            // this.out = new PrintWriter(this.socket.getOutputStream(), true);
            // this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String line3 = in.readLine();
            System.out.println("SYSTEM: CLIENT RECEIVED " + line3);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
