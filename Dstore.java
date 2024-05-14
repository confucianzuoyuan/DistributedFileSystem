import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Dstore {
    final int port;
    final int cport;
    int timeout;
    String fileFolder;
    ArrayList<String> filesStored;
    Socket controllerConnection;
    File dir;
    ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    CountDownLatch wait;

    public static void main(String[] args) {
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        var fileFolder = args[3];
        var filesStored = new ArrayList<String>();
        new Dstore(port, cport, timeout, fileFolder, filesStored);
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
            e.printStackTrace();
        }

        new Thread(this::ServerComms).start();

        try {
            var serverSocket = new ServerSocket(port);
            while (true) {
                try {
                    var clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(timeout);
                    new Thread(() -> {
                        try {
                            var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            String line;
                            while ((line = in.readLine()) != null) {
                                var words = line.split(" ");
                                var command = words[0];
                                switch (command) {
                                    case Protocol.STORE_TOKEN, Protocol.REBALANCE_STORE_TOKEN -> {
                                        var fileName = words[1];
                                        var fileSize = Integer.parseInt(words[2]);
                                        byte[] fileBuffer = new byte[fileSize];
                                        int buflen;
                                        var outputFile = new File(dir, fileName);
                                        var out = new FileOutputStream(outputFile);
                                        var fileInStream = clientSocket.getInputStream();
                                        Util.sendMessage(clientSocket, Protocol.ACK_TOKEN);
                                        while ((buflen = fileInStream.read(fileBuffer)) != -1) {
                                            out.write(fileBuffer, 0, buflen);
                                        }
                                        if (command.equals(Protocol.STORE_TOKEN)) {
                                            Util.sendMessage(controllerConnection, Protocol.STORE_ACK_TOKEN + " " + fileName);
                                        }
                                        filesStored.add(fileName);
                                        fileSizes.put(fileName, fileSize);
                                        fileInStream.close();
                                        out.close();
                                    }
                                    case Protocol.LOAD_DATA_TOKEN -> {
                                        if (!filesStored.contains(words[1])) {
                                            clientSocket.close();
                                        }
                                        sendFile(clientSocket, words[1]);
                                    }
                                    default -> System.out.println("Malformed message received: " + line);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void ServerComms() {
        try {
            // Sending
            controllerConnection = new Socket(InetAddress.getLocalHost(), cport);
            Util.sendMessage(controllerConnection, Protocol.JOIN_TOKEN + " " + port);

            // Receiving
            try {
                while (true) {
                    try {
                        var in = new BufferedReader(new InputStreamReader(controllerConnection.getInputStream()));
                        String line;
                        while ((line = in.readLine()) != null) {
                            var words = line.split(" ");
                            var command = words[0];
                            switch (command) {
                                case Protocol.LIST_TOKEN -> {
                                    var msg = new StringBuilder(Protocol.LIST_TOKEN);
                                    for (var file : filesStored) {
                                        msg.append(" ").append(file);
                                    }
                                    Util.sendMessage(controllerConnection, msg.toString());
                                }
                                case Protocol.REMOVE_TOKEN -> {
                                    var fileName = words[1];
                                    if (filesStored.contains(fileName)) {
                                        File toRemove = new File(dir, fileName);

                                        if (toRemove.delete()) {
                                            filesStored.remove(fileName);
                                            Util.sendMessage(controllerConnection, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                        }
                                    } else {
                                        Util.sendMessage(controllerConnection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                    }
                                }
                                case Protocol.REBALANCE_TOKEN -> {
                                    int noOfFiles = Integer.parseInt(words[1]);
                                    Hashtable<String, ArrayList<Integer>> filesToSend = new Hashtable<>();
                                    int offset = 2;
                                    for (int c = 0; c < noOfFiles; c++) {
                                        String fileName = words[offset];
                                        offset += 1;
                                        int noOfStores = Integer.parseInt(words[offset]);
                                        ArrayList<Integer> dStores = new ArrayList<>();
                                        for (int i = 1; i <= noOfStores; i++) {
                                            offset += 1;
                                            dStores.add(Integer.valueOf(words[offset]));
                                        }
                                        offset += 1;
                                        filesToSend.put(fileName, dStores);
                                    }

                                    for (String f : filesToSend.keySet()) {
                                        Integer fs = fileSizes.get(f);
                                        for (Integer d : filesToSend.get(f)) {
                                            Socket dSock = new Socket(InetAddress.getLocalHost(), d);
                                            Util.sendMessage(dSock, Protocol.REBALANCE_STORE_TOKEN + " " + f + " " + fs);
                                            wait = new CountDownLatch(1);
                                            new Thread(() -> {
                                                try {
                                                    BufferedReader in2 = new BufferedReader(
                                                            new InputStreamReader(dSock.getInputStream()));
                                                    String line2;
                                                    while ((line2 = in2.readLine()) != null) {
                                                        String[] splitIn2 = line2.split(" ");
                                                        if (splitIn2[0].equals(Protocol.ACK_TOKEN)) {
                                                            wait.countDown();
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }
                                            }).start();
                                            if (wait.await(timeout, TimeUnit.MILLISECONDS)) {
                                                sendFile(dSock, f);
                                            }
                                            dSock.close();
                                        }
                                    }
                                    int noToRemove = Integer.parseInt(words[offset]);
                                    offset += 1;
                                    for (int c = 0; c < noToRemove; c++) {
                                        if (filesStored.contains(words[offset])) {
                                            File toRemove = new File(dir, words[offset]);

                                            if (toRemove.delete()) {
                                                filesStored.remove(words[offset]);
                                            }
                                        } else {
                                            System.out.println("File " + words[offset] + " is not stored");
                                        }
                                        offset += 1;
                                    }
                                    Util.sendMessage(controllerConnection, Protocol.REMOVE_COMPLETE_TOKEN);
                                }
                                default -> System.out.println("Malformed Message");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void sendFile(Socket socket, String fileName) {
        try {
            var inputFile = new File(dir, fileName);
            var inputStream = new FileInputStream(inputFile);
            var dataOut = new DataOutputStream(socket.getOutputStream());

            byte[] fileContent = new byte[(int) inputFile.length()];
            inputStream.read(fileContent);
            dataOut.write(fileContent);
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}