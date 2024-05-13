import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {
    final int cport;
    int R;
    int timeout;
    int rebalancePeriod;
    public static Index index = new Index();
    CountDownLatch rebalanceDstoreCounterLatches;
    CountDownLatch rebalanceIsComplete;
    CountDownLatch isRebalancing = new CountDownLatch(0);

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
            rebalanceDstoreCounterLatches.countDown();
        } else if (index.dstoreSockets.size() < R) {
            sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            try {
                isRebalancing.await();
                var msg = new StringBuilder(Protocol.LIST_TOKEN);
                for (var fileName : index.fileStatus.keySet()) {
                    if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                        msg.append(" ").append(fileName);
                    }
                }
                sendMsg(socket, msg.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void store(String fileName, String fileSize, Socket socket) {
        try {
            isRebalancing.await();
            if (index.dstoreSockets.size() < R) {
                sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else if (index.fileStatus.containsKey(fileName)) {
                sendMsg(socket, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            } else {
                index.fileStatus.put(fileName, FileStatus.STORE_IN_PROGRESS);
                index.fileSizes.put(fileName, fileSize);
                var msg = new StringBuilder(Protocol.STORE_TO_TOKEN);

                /// 选择将文件保存到哪些dstores中
                int storeCount = 0;
                for (var dstorePort : index.dstoreFileLists.keySet()) {
                    if (storeCount == R) break;
                    if (!index.dstoreFileLists.get(dstorePort).contains(fileName) && storeCount < R) {
                        storeCount += 1;
                        index.dstoreFileLists.get(dstorePort).add(fileName);
                        msg.append(" ").append(dstorePort);
                    }
                }

                var countDown = new CountDownLatch(R);
                index.storeCountDownLatches.put(fileName, countDown);
                sendMsg(socket, msg.toString());
                try {
                    if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                        index.fileStatus.put(fileName, FileStatus.STORE_COMPLETE);
                        sendMsg(socket, Protocol.STORE_COMPLETE_TOKEN);
                        index.storeCountDownLatches.remove(fileName);
                    } else {
                        index.fileStatus.remove(fileName);
                        index.storeCountDownLatches.remove(fileName);
                    }
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void remove(String fileName, Socket socket) {
        try {
            isRebalancing.await();
            if (index.dstoreSockets.size() < R) {
                sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else if (!index.fileStatus.containsKey(fileName)) {
                sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                index.fileStatus.put(fileName, FileStatus.REMOVE_IN_PROGRESS);
                var countDown = new CountDownLatch(R);
                index.removeCountDownLatches.put(fileName, countDown);
                for (var dstorePort : index.dstoreFileLists.keySet()) {
                    if (index.dstoreFileLists.get(dstorePort).contains(fileName)) {
                        sendMsg(index.dstoreSockets.get(dstorePort), Protocol.REMOVE_TOKEN + " " + fileName);
                    }
                }

                try {
                    if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                        sendMsg(socket, Protocol.REMOVE_COMPLETE_TOKEN);
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
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void load(String command, String fileName, Socket socket) {
        try {
            isRebalancing.await();
            if (index.dstoreSockets.size() < R) {
                sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            } else if (!index.fileStatus.containsKey(fileName)) {
                sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                var isFileFound = false;
                if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                    if (command.equals(Protocol.LOAD_TOKEN)) {
                        index.fileDownloadHistory.put(fileName, new ArrayList<>());
                    }
                    for (var dstorePort : index.dstoreFileLists.keySet()) {
                        if (index.dstoreFileLists.get(dstorePort).contains(fileName) && !index.fileDownloadHistory.get(fileName).contains(dstorePort)) {
                            sendMsg(socket, Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + index.fileSizes.get(fileName));
                            index.fileDownloadHistory.get(fileName).add(dstorePort);
                            isFileFound = true;
                            break;
                        }
                    }

                    if (!isFileFound) {
                        sendMsg(socket, Protocol.ERROR_LOAD_TOKEN);
                    }
                } else {
                    sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        // Rebalance loop
        Timer timer = new Timer("ServerLoop");
        TimerTask task = new ServerTimerTask(this);
        try {
            timer.schedule(task, rebalancePeriod * 1000L, rebalancePeriod * 1000L);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // Receiver
        try {
            var serverSocket = new ServerSocket(cport);
            while (true) {
                try {
                    var clientSocket = serverSocket.accept();
                    Thread.ofVirtual().start(() -> {
                        try {
                            var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            String line;
                            while ((line = in.readLine()) != null) {
                                String[] words = line.split(" ");
                                switch (words[0]) {
                                    case Protocol.JOIN_TOKEN -> {
                                        Thread.ofVirtual().start(() -> {
                                            int dstorePort = Integer.parseInt(words[1]);
                                            index.dstoreSockets.put(dstorePort, clientSocket);
                                            index.dstoreFileLists.put(dstorePort, new ArrayList<>());
                                            try {
                                                isRebalancing.await();
                                                if (index.dstoreSockets.size() >= R) {
                                                    new Thread(task).start();
                                                }
                                            } catch (InterruptedException e) {
                                                System.out.println(e.getMessage());
                                            }
                                        });
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
                                            Thread.ofVirtual().start(() -> rebalanceIsComplete.countDown());
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
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMsg(Socket socket, String msg) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void rebalance() {
        if (!index.fileStatus.isEmpty()) {
            /// 如果有文件正在存储或者删除，则卡住
            while (index.fileStatus.containsValue(FileStatus.STORE_IN_PROGRESS) || index.fileStatus.containsValue(FileStatus.REMOVE_IN_PROGRESS)) {
            }
            isRebalancing = new CountDownLatch(1);
            if (index.dstoreSockets.size() >= R) {
                for (var dstorePort : index.dstoreSockets.keySet()) {
                    sendMsg(index.dstoreSockets.get(dstorePort), Protocol.LIST_TOKEN);
                }
                /// 等待所有dstores完成向Controller发送LIST FILES命令
                rebalanceDstoreCounterLatches = new CountDownLatch(index.dstoreSockets.size());
                try {
                    if (rebalanceDstoreCounterLatches.await(timeout, TimeUnit.MILLISECONDS)) {
                        double balanceNumber = (double) (R * index.fileStatus.size()) / index.dstoreSockets.size();
                        double floor = Math.floor(balanceNumber);
                        double ceil = Math.ceil(balanceNumber);
                        HashMap<String, Integer> fileStatistics = new HashMap<>();
                        for (var dstorePort : index.dstoreFileLists.keySet()) {
                            for (var fileName : index.dstoreFileLists.get(dstorePort)) {
                                fileStatistics.merge(fileName, 1, Integer::sum);
                            }
                        }

                        for (var dstoreSource : index.dstoreSockets.keySet()) {
                            HashMap<String, ArrayList<Integer>> send = new HashMap<>();
                            ArrayList<String> toRemove = new ArrayList<>();
                            int count = 0;
                            int dSize = index.dstoreFileLists.get(dstoreSource).size();
                            for (String f : index.dstoreFileLists.get(dstoreSource)) {
                                if (!index.fileStatus.containsKey(f) && !toRemove.contains(f)) {
                                    toRemove.add(f);
                                } else {
                                    var it = index.dstoreSockets.keySet().iterator();
                                    while (it.hasNext()) {
                                        Integer dSearch = it.next();
                                        if (index.dstoreFileLists.get(dSearch) != null && !dstoreSource.equals(dSearch)) {
                                            if (!toRemove.contains(f) && (dSize - toRemove.size()) > floor
                                                    && ((fileStatistics.get(f) == R && !index.dstoreFileLists.get(dSearch).contains(f)
                                                    && index.dstoreFileLists.get(dSearch).size() < ceil) || fileStatistics.get(f) > R)) {
                                                toRemove.add(f);
                                                fileStatistics.put(f, fileStatistics.get(f) - 1);
                                            }
                                            if (fileStatistics.get(f) < R) {
                                                if (!index.dstoreFileLists.get(dSearch).contains(f)
                                                        && (index.dstoreFileLists.get(dSearch).size() < ceil || !it.hasNext())) {
                                                    index.dstoreFileLists.get(dSearch).add(f);
                                                    fileStatistics.put(f, fileStatistics.get(f) + 1);
                                                    if (!send.containsKey(f)) {
                                                        send.put(f, new ArrayList<>());
                                                    }
                                                    send.get(f).add(dSearch);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            var message = new StringBuilder();
                            for (var file : send.keySet()) {
                                count += 1;
                                var files = new StringBuilder();
                                message.append(" ").append(file);
                                Integer noDStores = 0;
                                for (Integer ds : send.get(file)) {
                                    noDStores += 1;
                                    files.append(" ").append(ds);
                                }
                                message.append(" ").append(noDStores).append(files);
                            }
                            Integer countR = 0;
                            var files = new StringBuilder();
                            for (String r : toRemove) {
                                countR += 1;
                                files.append(" ").append(r);
                            }
                            message.append(" ").append(countR).append(files);
                            if (!send.isEmpty() || !toRemove.isEmpty()) {
                                rebalanceIsComplete = new CountDownLatch(1);
                                sendMsg(index.dstoreSockets.get(dstoreSource), Protocol.REBALANCE_TOKEN + " " + count + message);
                                var ignored = rebalanceIsComplete.await(timeout, TimeUnit.MILLISECONDS);
                            }
                        }
                    }
                } catch (NullPointerException | InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
            isRebalancing.countDown();
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