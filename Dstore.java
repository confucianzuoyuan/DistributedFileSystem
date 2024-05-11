import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Dstore {

    private static final Logger logger = Logger.getLogger(Dstore.class.getName());

    final int port;
    final int cport;
    int timeout;
    String fileFolder;
    ArrayList<String> filesStored;
    Socket toServer;
    File dir;
    HashMap<String, Integer> fileSizes = new HashMap<>();
    CountDownLatch wait;

    public static void main(String[] args) {
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];
        ArrayList<String> filesStored = new ArrayList<>();
        Dstore ignored = new Dstore(port, cport, timeout, fileFolder, filesStored);
    }

    public Dstore(int port, int cport, int timeout, String fileFolder, ArrayList<String> filesStored) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.filesStored = filesStored;
        this.dir = new File(fileFolder);
        try {
            dir.mkdirs();
            File[] files = dir.listFiles();
            for (File f : files) {
                f.delete();
            }
        } catch (Exception e) {
            logger.info("error " + e.getMessage());
        }

        new Thread(this::ServerComms).start();

        try {
            ServerSocket ss = new ServerSocket(port);
            for (; ; ) {
                try {
                    final Socket client = ss.accept();
                    client.setSoTimeout(timeout);
                    logger.info("New connection");
                    new Thread(new Runnable() {
                        public void run() {
                            try {
                                BufferedReader in = new BufferedReader(
                                        new InputStreamReader(client.getInputStream()));
                                String line;
                                logger.info("msg");
                                while ((line = in.readLine()) != null) {
                                    logger.info(line + " received");
                                    String[] splitIn = line.split(" ");
                                    String command = splitIn[0];
                                    switch (command) {
                                        case Protocol.STORE_TOKEN:
                                        case Protocol.REBALANCE_STORE_TOKEN:
                                            String fileName = splitIn[1];
                                            int size = Integer.parseInt(splitIn[2]);
                                            byte[] fileBuffer = new byte[size];
                                            int buflen;
                                            File outputFile = new File(dir, fileName);
                                            FileOutputStream out = new FileOutputStream(outputFile);
                                            InputStream fileInStream = client.getInputStream();
                                            logger.info("filename: " + fileName);
                                            sendMsg(client, Protocol.ACK_TOKEN);
                                            while ((buflen = fileInStream.read(fileBuffer)) != -1) {
                                                out.write(fileBuffer, 0, buflen);
                                            }
                                            if (command.equals(Protocol.STORE_TOKEN)) {
                                                sendMsg(toServer, Protocol.STORE_ACK_TOKEN + " " + fileName);
                                            }
                                            filesStored.add(fileName);
                                            fileSizes.put(fileName, size);
                                            fileInStream.close();
                                            out.close();
                                            break;
                                        case Protocol.LOAD_DATA_TOKEN:
                                            if (!filesStored.contains(splitIn[1])) {
                                                client.close();
                                            }
                                            sendFile(client, splitIn[1]);
                                            break;
                                        default:
                                            logger.info("Malformed message recived: " + line);
                                    }
                                }
                            } catch (Exception e) {
                                logger.info("Exception caught " + e.getMessage());
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    logger.info("Exception caught " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.info("Exception caught " + e.getMessage());
        }
    }

    private void ServerComms() {
        try {
            // Sending
            toServer = new Socket(InetAddress.getLocalHost(), cport);
            sendMsg(toServer, Protocol.JOIN_TOKEN + " " + port);

            // Receiving
            try {
                for (; ; ) {
                    try {
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(toServer.getInputStream()));
                        String line;
                        while ((line = in.readLine()) != null) {
                            logger.info(line + " received");
                            String[] splitIn = line.split(" ");
                            if (splitIn[0].equals(Protocol.LIST_TOKEN)) {
                                var msgToSend = new StringBuilder(Protocol.LIST_TOKEN);
                                for (String file : filesStored) {
                                    msgToSend.append(" ").append(file);
                                }
                                sendMsg(toServer, msgToSend.toString());
                                // REMOVE
                            } else if (splitIn[0].equals(Protocol.REMOVE_TOKEN)) {
                                String fileName = splitIn[1];
                                if (filesStored.contains(fileName)) {
                                    File toRemove = new File(dir, fileName);

                                    if (toRemove.delete()) {
                                        filesStored.remove(fileName);
                                        logger.info("File " + fileName + " removed");
                                        sendMsg(toServer, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                    }
                                } else {
                                    sendMsg(toServer, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                    logger.info("File " + fileName + " is not stored");
                                }
                            } else if (splitIn[0].equals(Protocol.REBALANCE_TOKEN)) {
                                int noOfFiles = Integer.parseInt(splitIn[1]);
                                Hashtable<String, ArrayList<Integer>> filesToSend = new Hashtable<>();
                                int offset = 2;
                                for (int c = 0; c < noOfFiles; c++) {
                                    String fileName = splitIn[offset];
                                    offset += 1;
                                    int noOfStores = Integer.parseInt(splitIn[offset]);
                                    ArrayList<Integer> dStores = new ArrayList<>();
                                    for (int i = 1; i <= noOfStores; i++) {
                                        offset += 1;
                                        dStores.add(Integer.valueOf(splitIn[offset]));
                                    }
                                    offset += 1;
                                    filesToSend.put(fileName, dStores);
                                }

                                for (String f : filesToSend.keySet()) {
                                    Integer fs = fileSizes.get(f);
                                    for (Integer d : filesToSend.get(f)) {
                                        Socket dSock = new Socket(InetAddress.getLocalHost(), d);
                                        sendMsg(dSock, Protocol.REBALANCE_STORE_TOKEN + " " + f + " " + fs);
                                        wait = new CountDownLatch(1);
                                        new Thread(new Runnable() {
                                            public void run() {
                                                try {
                                                    BufferedReader in2 = new BufferedReader(
                                                            new InputStreamReader(dSock.getInputStream()));
                                                    String line2;
                                                    logger.info("msg");
                                                    while ((line2 = in2.readLine()) != null) {
                                                        logger.info(line2 + " received");
                                                        String[] splitIn2 = line2.split(" ");
                                                        if (splitIn2[0].equals(Protocol.ACK_TOKEN)) {
                                                            wait.countDown();
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    logger.info("error: " + e.getMessage());
                                                }
                                            }
                                        }).start();
                                        if (wait.await(timeout, TimeUnit.MILLISECONDS)) {
                                            sendFile(dSock, f);
                                        }
                                        dSock.close();
                                    }
                                }
                                int noToRemove = Integer.parseInt(splitIn[offset]);
                                offset += 1;
                                for (int c = 0; c < noToRemove; c++) {
                                    if (filesStored.contains(splitIn[offset])) {
                                        File toRemove = new File(dir, splitIn[offset]);

                                        if (toRemove.delete()) {
                                            filesStored.remove(splitIn[offset]);
                                            logger.info("File " + splitIn[offset] + " removed");
                                        }
                                    } else {
                                        logger.info("File " + splitIn[offset] + " is not stored");
                                    }
                                    offset += 1;
                                }
                                sendMsg(toServer, Protocol.REMOVE_COMPLETE_TOKEN);
                                logger.info("End");

                            } else {
                                logger.info("Malformed message received: " + line);
                            }
                        }
                    } catch (Exception e) {
                        logger.info("error " + e);
                    }
                }
            } catch (Exception e) {
                logger.info("error " + e);
            }
        } catch (Exception e) {
            logger.info("error" + e);
        }
    }

    private void sendMsg(Socket socket, String msg) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
            logger.info("TCP message " + msg + " sent");
        } catch (Exception e) {
            logger.info("error" + e);
        }
    }

    private void sendFile(Socket socket, String fileName) {
        try {
            File inputFile = new File(dir, fileName);
            FileInputStream inputStream = new FileInputStream(inputFile);
            DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());

            byte[] fileContent = new byte[(int) inputFile.length()];
            inputStream.read(fileContent);
            dataOut.write(fileContent);
            logger.info(fileName + " file sent");
            inputStream.close();
        } catch (Exception e) {
            logger.info("error " + e.getMessage());
        }
    }
}