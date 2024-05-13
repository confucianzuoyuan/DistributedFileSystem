import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Controller {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());

    final int cport;
    int R;
    int timeout;
    int rebalancePeriod;
    public static Index index = new Index();
    Boolean balancing = false;
    CountDownLatch rebaLatch;
    CountDownLatch rebaCompLatch;

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
            System.out.println("Files " + files + " added for " + port);
            try {
                rebaLatch.countDown();
            } catch (NullPointerException e) {
            }
        } else if (index.dstoreSockets.size() < R) {
            sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            while (balancing) {
            }
            logger.info("LIST from Client");
            var toSend = new StringBuilder(Protocol.LIST_TOKEN);
            for (var fileName : index.fileStatus.keySet()) {
                if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                    toSend.append(" ").append(fileName);
                }
            }
            sendMsg(socket, toSend.toString());
        }
    }

    private void store(String fileName, String fileSize, Socket socket) {
        while (balancing) {
        }
        if (index.dstoreSockets.size() < R) {
            sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (index.fileStatus.containsKey(fileName)) {
            sendMsg(socket, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
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
            sendMsg(socket, toSend.toString());
            logger.info("Thread paused");
            try {
            if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                index.fileStatus.put(fileName, FileStatus.STORE_COMPLETE);
                sendMsg(socket, Protocol.STORE_COMPLETE_TOKEN);
                index.storeCountDownLatches.remove(fileName);
                logger.info("Index updated for " + fileName);
            } else {
                logger.info("STORE " + fileName + " timeout " + countDown.getCount());
                index.fileStatus.remove(fileName);
                index.storeCountDownLatches.remove(fileName);
            }} catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    private void remove(String filename, Socket socket) {
        while (balancing) {
        }
        if (index.dstoreSockets.size() >= R) {
            try {
                if (index.fileStatus.get(filename) == FileStatus.STORE_COMPLETE) {
                    index.fileStatus.put(filename, FileStatus.REMOVE_IN_PROGRESS);
                    var countDown = new CountDownLatch(R);
                    index.removeCountDownLatches.put(filename, countDown);
                    for (var dstorePort : index.dstoreFileLists.keySet()) {
                        if (index.dstoreFileLists.get(dstorePort).contains(filename)) {
                            sendMsg(index.dstoreSockets.get(dstorePort), Protocol.REMOVE_TOKEN + " " + filename);
                        }
                    }

                    logger.info("Thread paused");
                    try {
                        if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                            sendMsg(socket, Protocol.REMOVE_COMPLETE_TOKEN);
                            index.fileStatus.remove(filename);
                            index.removeCountDownLatches.remove(filename);
                            index.fileSizes.remove(filename);
                            logger.info("Index removed for " + filename);
                        } else {
                            logger.info("REMOVE " + filename + " timeout " + countDown.getCount());
                            index.removeCountDownLatches.remove(filename);
                        }
                    } catch (InterruptedException | NullPointerException e) {
                        logger.info("error " + e.getMessage());
                    }
                } else {
                    sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            } catch (NullPointerException e) {
                sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            }
        } else {
            sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        }
    }

    private void load(String command, String fileName, Socket socket) {
        while (balancing) {
        }
        var notFoundFlag = true;
        if (index.dstoreSockets.size() >= R) {
            try {
                if (index.fileStatus.get(fileName) == FileStatus.STORE_COMPLETE) {
                    if (command.equals(Protocol.LOAD_TOKEN)) {
                        index.fileDownloadHistory.put(fileName, new ArrayList<>());
                    }
                    for (Integer store : index.dstoreFileLists.keySet()) {
                        if (index.dstoreFileLists.get(store).contains(fileName) && !index.fileDownloadHistory.get(fileName).contains(store)) {
                            sendMsg(socket, Protocol.LOAD_FROM_TOKEN + " " + store + " " + index.fileSizes.get(fileName));
                            index.fileDownloadHistory.get(fileName).add(store);
                            notFoundFlag = false;
                            break;
                        }
                    }
                    if (notFoundFlag) {
                        sendMsg(socket, Protocol.ERROR_LOAD_TOKEN);
                    }

                } else {
                    sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            } catch (NullPointerException e) {
                sendMsg(socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            }
        } else {
            sendMsg(socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
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
            logger.info("Catching: " + e.getMessage());
        }

        // Receiver
        try {
            ServerSocket ss = new ServerSocket(cport);
            for (; ; ) {
                try {
                    final Socket client = ss.accept();
                    logger.info("New connection");
                    new Thread(new Runnable() {
                        public void run() {
                            try {
                                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                String line;
                                while ((line = in.readLine()) != null) {
                                    logger.info("Message received: " + line);
                                    String[] words = line.split(" ");
                                    switch (words[0]) {
                                        case Protocol.JOIN_TOKEN -> {
                                            int dstorePort = Integer.parseInt(words[1]);
                                            index.dstoreSockets.put(dstorePort, client);
                                            index.dstoreFileLists.put(dstorePort, new ArrayList<>());
                                            if (index.dstoreSockets.size() >= R && !balancing) {
                                                new Thread(task).start();
                                            }
                                        }
                                        case Protocol.LIST_TOKEN -> {
                                            String finalLine = line;
                                            Thread.ofVirtual().start(() -> list(client.getPort(), finalLine, client));
                                        }
                                        case Protocol.STORE_TOKEN ->
                                                Thread.ofVirtual().start(() -> store(words[1], words[2], client));
                                        case Protocol.REMOVE_TOKEN ->
                                                Thread.ofVirtual().start(() -> remove(words[1], client));
                                        case Protocol.STORE_ACK_TOKEN ->
                                                Thread.ofVirtual().start(() -> index.storeCountDownLatches.get(words[1]).countDown());
                                        case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                                            var finalPort = client.getPort();
                                            Thread.ofVirtual().start(() -> {
                                                var filename = words[1];
                                                index.removeCountDownLatches.get(filename).countDown();
                                                index.dstoreFileLists.get(finalPort).remove(filename);
                                            });
                                        }
                                        case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN ->
                                                Thread.ofVirtual().start(() -> load(words[0], words[1], client));
                                        case Protocol.REBALANCE_COMPLETE_TOKEN ->
                                                Thread.ofVirtual().start(() -> rebaCompLatch.countDown());
                                        default -> System.out.println("Malformed Message");
                                    }
                                }

                            } catch (SocketException e) {
                                logger.info("error " + e.getMessage());
                                if (client.getPort() != 0) {
                                    index.dstoreSockets.remove(client.getPort());
                                    index.dstoreFileLists.remove(client.getPort());
                                    logger.info("Removed a Dstore");
                                    if (index.dstoreSockets.isEmpty()) {
                                        index.fileStatus.clear();
                                        index.fileSizes.clear();
                                    }
                                }
                                try {
                                    client.close();
                                } catch (IOException e1) {
                                    logger.info("error " + e1.getMessage());
                                }
                            } catch (IOException e) {
                                logger.info("error " + e.getMessage());
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    logger.info("error " + e);
                }
            }
        } catch (Exception e) {
            logger.info("error " + e);
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

    public void rebalance() {
        if (!index.fileStatus.isEmpty()) {
            while (index.fileStatus.containsValue(FileStatus.STORE_IN_PROGRESS) || index.fileStatus.containsValue(FileStatus.REMOVE_IN_PROGRESS)) {
            }
            balancing = true;
            if (index.dstoreSockets.size() >= R) {
                for (var s : index.dstoreSockets.keySet()) {
                    sendMsg(index.dstoreSockets.get(s), Protocol.LIST_TOKEN);
                }
                rebaLatch = new CountDownLatch(index.dstoreSockets.size());
                try {
                    if (rebaLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                        double balanceNumber = (double) (R * index.fileStatus.size()) / index.dstoreSockets.size();
                        double floor = Math.floor(balanceNumber);
                        double ceil = Math.ceil(balanceNumber);
                        Hashtable<String, Integer> seen = new Hashtable<>();
                        for (var d : index.dstoreFileLists.keySet()) {
                            for (var f : index.dstoreFileLists.get(d)) {
                                seen.merge(f, 1, Integer::sum);
                            }
                        }

                        for (Integer d : index.dstoreSockets.keySet()) {
                            HashMap<String, ArrayList<Integer>> send = new HashMap<>();
                            ArrayList<String> toRemove = new ArrayList<>();
                            int count = 0;
                            int dSize = index.dstoreFileLists.get(d).size();
                            for (String f : index.dstoreFileLists.get(d)) {
                                if (!index.fileStatus.containsKey(f) && !toRemove.contains(f)) {
                                    toRemove.add(f);
                                } else {
                                    var it = index.dstoreSockets.keySet().iterator();
                                    while (it.hasNext()) {
                                        Integer dSearch = it.next();
                                        if (index.dstoreFileLists.get(dSearch) != null && !d.equals(dSearch)) {
                                            if (!toRemove.contains(f) && (dSize - toRemove.size()) > floor
                                                    && ((seen.get(f) == R && !index.dstoreFileLists.get(dSearch).contains(f)
                                                    && index.dstoreFileLists.get(dSearch).size() < ceil) || seen.get(f) > R)) {
                                                toRemove.add(f);
                                                seen.put(f, seen.get(f) - 1);
                                            }
                                            if (seen.get(f) < R) {
                                                if (!index.dstoreFileLists.get(dSearch).contains(f)
                                                        && (index.dstoreFileLists.get(dSearch).size() < ceil || !it.hasNext())) {
                                                    index.dstoreFileLists.get(dSearch).add(f);
                                                    seen.put(f, seen.get(f) + 1);
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
                            for (String f : send.keySet()) {
                                count += 1;
                                var files = new StringBuilder();
                                message.append(" ").append(f);
                                Integer noDStores = 0;
                                for (Integer ds : send.get(f)) {
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
                                rebaCompLatch = new CountDownLatch(1);
                                sendMsg(index.dstoreSockets.get(d), Protocol.REBALANCE_TOKEN + " " + count + message);
                                if (rebaCompLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                                }
                            }
                        }
                    }
                } catch (NullPointerException | InterruptedException e) {
                    logger.info("error " + e.getMessage());
                }
            }
            logger.info("Balance finished ---------");
            balancing = false;
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