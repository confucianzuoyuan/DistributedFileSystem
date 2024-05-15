import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Dstore {
    private int port;
    private int cport;
    int timeout;
    String fileFolder;
    ArrayList<String> filesInDstore;
    Socket controllerConnection;
    File dir;
    ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    CountDownLatch waitForSendFileToDstore;

    public static void main(String[] args) {
        if (args.length != 4) {
            throw new RuntimeException("wrong number of args");
        }
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        var fileFolder = args[3];
        var filesStored = new ArrayList<String>();
        new Dstore(port, cport, timeout, fileFolder, filesStored);
    }

    public Dstore(int port, int cport, int timeout, String fileFolder, ArrayList<String> filesInDstore) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.filesInDstore = filesInDstore;
        this.dir = new File(fileFolder);
        cleanDirectory(dir);

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
                                        receiveFile(clientSocket, words, dir, controllerConnection);
                                    }
                                    case Protocol.LOAD_DATA_TOKEN -> {
                                        if (!filesInDstore.contains(words[1])) {
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
                                        if (filesInDstore.contains(fileToRemove)) {
                                            var file = new File(dir, fileToRemove);
                                            if (file.delete()) {
                                                filesInDstore.remove(fileToRemove);
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
        for (var file : filesInDstore) {
            msg.append(" ").append(file);
        }
        Util.sendMessage(controllerConnection, msg.toString());
    }

    private void removeFileInDstore(String fileName, Socket controllerConnection) {
        if (filesInDstore.contains(fileName)) {
            var toRemove = new File(dir, fileName);

            if (toRemove.delete()) {
                filesInDstore.remove(fileName);
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

    public static void cleanDirectory(File dir) {
        try {
            if (!dir.exists()) {
                boolean dirsCreated = dir.mkdirs();
                if (!dirsCreated) {
                    System.err.println("Failed to create directories: " + dir.getAbsolutePath());
                    return;
                }
            }

            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    Files.delete(f.toPath());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void receiveFile(Socket clientSocket, String[] words, File dir, Socket controllerConnection) {
        String fileName = words[1];
        int fileSize = Integer.parseInt(words[2]);
        File outputFile = new File(dir, fileName);

        // 使用try-with-resources自动管理资源
        try (InputStream fileInStream = clientSocket.getInputStream();
             FileOutputStream out = new FileOutputStream(outputFile)) {

            Util.sendMessage(clientSocket, Protocol.ACK_TOKEN);

            // 直接从输入流读取到输出流
            byte[] buffer = new byte[4096]; // 使用更小的缓冲区
            int bytesRead;
            while ((bytesRead = fileInStream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }

            // 文件传输完成后，根据命令发送确认消息
            if (Protocol.STORE_TOKEN.equals(words[0])) {
                Util.sendMessage(controllerConnection, Protocol.STORE_ACK_TOKEN + " " + fileName);
            }

            // 更新文件信息
            filesInDstore.add(fileName);
            fileSizes.put(fileName, fileSize);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}