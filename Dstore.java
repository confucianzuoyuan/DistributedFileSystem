import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
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
    CountDownLatch waitForSendFileToDstore;

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

        /// Dstore to Controller Socket
        new Thread(this::ConnectionToController).start();

        /// Dstore Socket
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

    private void ConnectionToController() {
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
                                case Protocol.LIST_TOKEN -> listFilesInDstore(controllerConnection);
                                case Protocol.REMOVE_TOKEN -> removeFileInDstore(words[1], controllerConnection);
                                case Protocol.REBALANCE_TOKEN -> {
                                    // files_to_send is the list of files to send and is in the form
                                    // number_of_files_to_send file_to_send_1 file_to_send_2 ... file_to_send_N
                                    // and file_to_send_i is in the form filename number_of_dstores
                                    // dstore1 dstore2 ... dstoreM
                                    var t = parseSendFilesAndRemoveFiles(line);
                                    var filesToSend = t.filesToSendList;

                                    for (var fileToSend : filesToSend) {
                                        var fileSize = fileSizes.get(fileToSend.fileName);
                                        for (var dstorePort : fileToSend.dstores) {
                                            var dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(dstorePort));
                                            Util.sendMessage(dstoreSocket, Protocol.REBALANCE_STORE_TOKEN + " " + fileToSend.fileName + " " + fileSize);
                                            waitForSendFileToDstore = new CountDownLatch(1);
                                            Thread.ofVirtual().start(() -> sendFileToDstore(dstoreSocket));
                                            if (waitForSendFileToDstore.await(timeout, TimeUnit.MILLISECONDS)) {
                                                sendFile(dstoreSocket, fileToSend.fileName);
                                            }
                                            dstoreSocket.close();
                                        }
                                    }

                                    var filesToRemove = t.filesToRemoveList;
                                    for (var fileToRemove : filesToRemove) {
                                        if (filesStored.contains(fileToRemove)) {
                                            var file = new File(dir, fileToRemove);
                                            if (file.delete()) {
                                                filesStored.remove(fileToRemove);
                                            }
                                        }
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

    private void sendFileToDstore(Socket dstoreSocket) {
        try {
            var in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                var parts = line.split(" ");
                if (parts[0].equals(Protocol.ACK_TOKEN)) {
                    waitForSendFileToDstore.countDown();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void listFilesInDstore(Socket controllerConnection) {
        var msg = new StringBuilder(Protocol.LIST_TOKEN);
        for (var file : filesStored) {
            msg.append(" ").append(file);
        }
        Util.sendMessage(controllerConnection, msg.toString());
    }

    private void removeFileInDstore(String fileName, Socket controllerConnection) {
        if (filesStored.contains(fileName)) {
            var toRemove = new File(dir, fileName);

            if (toRemove.delete()) {
                filesStored.remove(fileName);
                Util.sendMessage(controllerConnection, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
            }
        } else {
            Util.sendMessage(controllerConnection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
        }
    }

    private void sendFile(Socket socket, String fileName) {
        // 使用try-with-resources自动管理资源
        try (var ignored = new FileInputStream(new File(dir, fileName));
        ) {
            var dataOut = new DataOutputStream(socket.getOutputStream());
            // 直接使用Files.readAllBytes来读取文件内容到字节数组
            byte[] fileContent = Files.readAllBytes(new File(dir, fileName).toPath());

            dataOut.write(fileContent);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class FileToSend {
        String fileName;
        List<String> dstores;

        public FileToSend(String fileName, List<String> dstores) {
            this.fileName = fileName;
            this.dstores = dstores;
        }

        @Override
        public String toString() {
            return "FileToSend{" +
                    "fileName='" + fileName + '\'' +
                    ", dstores=" + dstores +
                    '}';
        }
    }

    private static class FilesToSendAndToRemove {
        public List<FileToSend> filesToSendList;
        public List<String> filesToRemoveList;

        public FilesToSendAndToRemove(List<FileToSend> filesToSendList, List<String> filesToRemoveList) {
            this.filesToSendList = filesToSendList;
            this.filesToRemoveList = filesToRemoveList;
        }
    }

    public static FilesToSendAndToRemove parseSendFilesAndRemoveFiles(String line) {
        String[] parts = line.split(" ");
        int index = 0;
        int numberOfFilesToSend = Integer.parseInt(parts[index++]);
        List<FileToSend> filesToSendList = new ArrayList<>();

        // 解析filesToSend部分
        for (int i = 0; i < numberOfFilesToSend; i++) {
            String fileName = parts[index++];
            int numberOfDstores = Integer.parseInt(parts[index++]);
            List<String> dstores = new ArrayList<>();
            for (int j = 0; j < numberOfDstores; j++) {
                dstores.add(parts[index++]);
            }
            filesToSendList.add(new FileToSend(fileName, dstores));
        }

        // 解析filesToRemove部分
        List<String> filesToRemoveList = new ArrayList<>();
        int numberOfFilesToRemove = Integer.parseInt(parts[index++]);
        for (int i = 0; i < numberOfFilesToRemove; i++) {
            filesToRemoveList.add(parts[index++]);
        }

        return new FilesToSendAndToRemove(filesToSendList, filesToRemoveList);
    }
}