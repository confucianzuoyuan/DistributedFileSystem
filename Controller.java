import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class Controller {
    final int cport;
    int R;
    int timeout;
    int rebalancePeriod;
    public static Index index = new Index();
    boolean isRebalancing = false;
    CountDownLatch controllerGetAllListCommandFromDstores;
    CountDownLatch oneDstoreCompleteRebalance;

    public static void main(String[] args) {
        final int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);
        new Controller(cport, R, timeout, rebalancePeriod);
    }

    private void list(int port, String command, Socket socket) {
        var words = command.split(" ");
        if (index.dstoreSockets.containsKey(port)) {
            var files = new ArrayList<>(Arrays.asList(words).subList(1, words.length));
            index.dstoreFileLists.remove(port);
            index.dstoreFileLists.put(port, files);
            controllerGetAllListCommandFromDstores.countDown();
        } else if (index.dstoreSockets.size() < R) {
            Util.sendMessage(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            while (isRebalancing) {
            }
            var msg = new StringBuilder(Protocol.LIST_TOKEN);
            for (var fileName : index.fileStatus.keySet()) {
                if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                    msg.append(" ").append(fileName);
                }
            }
            Util.sendMessage(socket, msg.toString());
        }
    }

    private void store(String fileName, String fileSize, Socket socket) {
        while (isRebalancing) {
        }
        if (index.dstoreSockets.size() < R) {
            Util.sendMessage(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (index.fileStatus.containsKey(fileName)) {
            Util.sendMessage(socket, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        } else {
            index.fileStatus.put(fileName, FileStatus.STORE_IN_PROGRESS);
            index.fileSizes.put(fileName, fileSize);
            var toSend = new StringBuilder(Protocol.STORE_TO_TOKEN);

            /// 选择将文件保存到哪些dstores中
            int storeCount = 0;
            for (var dstorePort : index.dstoreFileLists.keySet()) {
                if (storeCount == R) break;
                if (!index.dstoreFileLists.get(dstorePort).contains(fileName) && storeCount < R) {
                    storeCount += 1;
                    index.dstoreFileLists.get(dstorePort).add(fileName);
                    toSend.append(" ").append(dstorePort);
                }
            }

            var countDown = new CountDownLatch(R);
            index.storeCountDownLatches.put(fileName, countDown);
            Util.sendMessage(socket, toSend.toString());
            try {
                if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                    index.fileStatus.put(fileName, FileStatus.STORE_COMPLETE);
                    Util.sendMessage(socket, Protocol.STORE_COMPLETE_TOKEN);
                    index.storeCountDownLatches.remove(fileName);
                } else {
                    index.fileStatus.remove(fileName);
                    index.storeCountDownLatches.remove(fileName);
                }
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private void remove(String fileName, Socket socket) {
        while (isRebalancing) {
        }
        if (index.dstoreSockets.size() < R) {
            Util.sendMessage(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!index.fileStatus.containsKey(fileName)) {
            Util.sendMessage(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            index.fileStatus.put(fileName, FileStatus.REMOVE_IN_PROGRESS);
            var countDown = new CountDownLatch(R);
            index.removeCountDownLatches.put(fileName, countDown);
            for (var dstorePort : index.dstoreFileLists.keySet()) {
                if (index.dstoreFileLists.get(dstorePort).contains(fileName)) {
                    Util.sendMessage(index.dstoreSockets.get(dstorePort), Protocol.REMOVE_TOKEN + " " + fileName);
                }
            }

            try {
                if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                    Util.sendMessage(socket, Protocol.REMOVE_COMPLETE_TOKEN);
                    index.fileStatus.remove(fileName);
                    index.removeCountDownLatches.remove(fileName);
                    index.fileSizes.remove(fileName);
                } else {
                    index.removeCountDownLatches.remove(fileName);
                }
            } catch (InterruptedException | NullPointerException e) {
                System.out.println("error " + e.getMessage());
            }
        }
    }

    private void load(String command, String fileName, Socket socket) {
        while (isRebalancing) {
        }
        if (index.dstoreSockets.size() < R) {
            Util.sendMessage(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!index.fileStatus.containsKey(fileName)) {
            Util.sendMessage(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            var fileNotFound = true;
            if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                if (command.equals(Protocol.LOAD_TOKEN)) {
                    index.fileDownloadHistory.put(fileName, new ArrayList<>());
                }
                for (var dstorePort : index.dstoreFileLists.keySet()) {
                    if (index.dstoreFileLists.get(dstorePort).contains(fileName) && !index.fileDownloadHistory.get(fileName).contains(dstorePort)) {
                        Util.sendMessage(socket, Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + index.fileSizes.get(fileName));
                        index.fileDownloadHistory.get(fileName).add(dstorePort);
                        fileNotFound = false;
                        break;
                    }
                }
                if (fileNotFound) {
                    Util.sendMessage(socket, Protocol.ERROR_LOAD_TOKEN);
                }

            } else {
                Util.sendMessage(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            }
        }
    }

    private Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        // Rebalance loop
        var timer = new Timer("ServerLoop");
        var task = new ServerTimerTask(this);
        try {
            timer.schedule(task, rebalancePeriod * 1000L, rebalancePeriod * 1000L);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Receiver
        try {
            var serverSocket = new ServerSocket(cport);
            while (true) {
                try {
                    var clientSocket = serverSocket.accept();
                    new Thread(() -> {
                        try {
                            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            String line;
                            while ((line = in.readLine()) != null) {
                                String[] words = line.split(" ");
                                switch (words[0]) {
                                    case Protocol.JOIN_TOKEN -> {
                                        int dstorePort = Integer.parseInt(words[1]);
                                        index.dstoreSockets.put(dstorePort, clientSocket);
                                        index.dstoreFileLists.put(dstorePort, new ArrayList<>());
                                        if (index.dstoreSockets.size() >= R && !isRebalancing) {
                                            new Thread(task).start();
                                        }
                                    }
                                    case Protocol.LIST_TOKEN -> {
                                        String finalLine = line;
                                        Thread.ofVirtual().start(() -> list(clientSocket.getPort(), finalLine, clientSocket));
                                    }
                                    case Protocol.STORE_TOKEN ->
                                            Thread.ofVirtual().start(() -> store(words[1], words[2], clientSocket));
                                    case Protocol.REMOVE_TOKEN ->
                                            Thread.ofVirtual().start(() -> remove(words[1], clientSocket));
                                    case Protocol.STORE_ACK_TOKEN ->
                                            Thread.ofVirtual().start(() -> index.storeCountDownLatches.get(words[1]).countDown());
                                    case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                                        Thread.ofVirtual().start(() -> {
                                            var filename = words[1];
                                            index.removeCountDownLatches.get(filename).countDown();
                                            /// 这里index.dstoreFileLists.get(client.getPort())可能为null空指针
                                            if (index.dstoreSockets.containsKey(clientSocket.getPort())) {
                                                index.dstoreFileLists.get(clientSocket.getPort()).remove(filename);
                                            }
                                        });
                                    }
                                    case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN ->
                                            Thread.ofVirtual().start(() -> load(words[0], words[1], clientSocket));
                                    case Protocol.REBALANCE_COMPLETE_TOKEN ->
                                            Thread.ofVirtual().start(() -> oneDstoreCompleteRebalance.countDown());
                                    default -> System.out.println("Malformed Message");
                                }
                            }

                        } catch (SocketException e) {
                            if (clientSocket.getPort() != 0) {
                                index.dstoreSockets.remove(clientSocket.getPort());
                                index.dstoreFileLists.remove(clientSocket.getPort());
                                if (index.dstoreSockets.isEmpty()) {
                                    index.fileStatus.clear();
                                    index.fileSizes.clear();
                                }
                            }
                            try {
                                clientSocket.close();
                            } catch (IOException closeError) {
                                closeError.printStackTrace();
                            }
                        } catch (IOException e) {
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

    public void rebalance() {
        if (!index.fileStatus.isEmpty()) {
            while (index.fileStatus.containsValue(FileStatus.STORE_IN_PROGRESS) || index.fileStatus.containsValue(FileStatus.REMOVE_IN_PROGRESS)) {
            }
            isRebalancing = true;
            if (index.dstoreSockets.size() >= R) {
                for (var s : index.dstoreSockets.keySet()) {
                    Util.sendMessage(index.dstoreSockets.get(s), Protocol.LIST_TOKEN);
                }
                controllerGetAllListCommandFromDstores = new CountDownLatch(index.dstoreSockets.size());
                try {
                    if (controllerGetAllListCommandFromDstores.await(timeout, TimeUnit.MILLISECONDS)) {
                        var fileNumberInEveryDstore = (double) (R * index.fileStatus.size()) / index.dstoreSockets.size();
                        var floor = Math.floor(fileNumberInEveryDstore);
                        var ceil = Math.ceil(fileNumberInEveryDstore);
                        var seen = new HashMap<String, Integer>();
                        for (var d : index.dstoreFileLists.keySet()) {
                            for (var f : index.dstoreFileLists.get(d)) {
                                seen.merge(f, 1, Integer::sum);
                            }
                        }

                        for (var dstorePort : index.dstoreSockets.keySet()) {
                            var filesToSend = new HashMap<String, ArrayList<Integer>>();
                            var filesToRemove = new ArrayList<String>();
                            int fileNumberInDstore = index.dstoreFileLists.get(dstorePort).size();
                            for (var fileName : index.dstoreFileLists.get(dstorePort)) {
                                if (!index.fileStatus.containsKey(fileName) && !filesToRemove.contains(fileName)) {
                                    filesToRemove.add(fileName);
                                } else {
                                    int size = index.dstoreSockets.size();
                                    int dstoreCount = 0;
                                    for (var targetDstore : index.dstoreSockets.keySet()) {
                                        dstoreCount++;
                                        if (!index.dstoreFileLists.containsKey(targetDstore)) {
                                            continue;
                                        }
                                        if (dstorePort.equals(targetDstore)) {
                                            continue;
                                        }
                                        if (!filesToRemove.contains(fileName) && (fileNumberInDstore - filesToRemove.size()) > floor
                                                && ((seen.get(fileName) == R && !index.dstoreFileLists.get(targetDstore).contains(fileName)
                                                && index.dstoreFileLists.get(targetDstore).size() < ceil) || seen.get(fileName) > R)) {
                                            filesToRemove.add(fileName);
                                            seen.put(fileName, seen.get(fileName) - 1);
                                        }
                                        if (seen.get(fileName) < R) {
                                            if (!index.dstoreFileLists.get(targetDstore).contains(fileName)
                                                    && (index.dstoreFileLists.get(targetDstore).size() < ceil || dstoreCount == size)) {
                                                index.dstoreFileLists.get(targetDstore).add(fileName);
                                                seen.put(fileName, seen.get(fileName) + 1);
                                                if (!filesToSend.containsKey(fileName)) {
                                                    filesToSend.put(fileName, new ArrayList<>());
                                                }
                                                filesToSend.get(fileName).add(targetDstore);
                                            }
                                        }
                                    }
                                }
                            }

                            /// "REBALANCE 2 f1 2 p1 p2 f2 1 p3 2 f2 f3"
                            var message = new StringBuilder();
                            message.append(" ").append(filesToSend.size());
                            for (var fileToSend : filesToSend.keySet()) {
                                message.append(" ").append(fileToSend).append(" ").append(filesToSend.get(fileToSend).size());
                                for (var dPort : filesToSend.get(fileToSend)) {
                                    message.append(" ").append(dPort);
                                }
                            }

                            message.append(" ").append(filesToRemove.size());
                            for (var fileToRemove : filesToRemove) {
                                message.append(" ").append(fileToRemove);
                            }

                            if (!filesToSend.isEmpty() || !filesToRemove.isEmpty()) {
                                oneDstoreCompleteRebalance = new CountDownLatch(1);
                                Util.sendMessage(index.dstoreSockets.get(dstorePort), Protocol.REBALANCE_TOKEN + message);
                                var ignored = oneDstoreCompleteRebalance.await(timeout, TimeUnit.MILLISECONDS);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Balance finished ---------");
            isRebalancing = false;
        }
    }
}

class ServerTimerTask extends TimerTask {
    private Controller c;
    private Boolean balancingL = false;

    ServerTimerTask(Controller c) {
        this.c = c;
    }

    @Override
    public void run() {
        if (!balancingL && c.index.dstoreSockets.size() >= c.R) {
            balancingL = true;
            c.rebalance();
            balancingL = false;
        }
    }
}