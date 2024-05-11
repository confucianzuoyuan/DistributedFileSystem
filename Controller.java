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
    HashMap<Integer, Socket> dstores = new HashMap<>();
    HashMap<Integer, ArrayList<String>> fileLocations = new HashMap<>();
    HashMap<String, FileStatusInIndex> index = new HashMap<>();
    HashMap<String, CountDownLatch> locksS = new HashMap<>();
    HashMap<String, CountDownLatch> locksR = new HashMap<>();
    HashMap<String, String> fileSizes = new HashMap<>();
    ArrayList<Integer> lastDStore = new ArrayList<>();
    Boolean balancing = false;
    CountDownLatch rebaLatch;
    CountDownLatch rebaCompLatch;

    public static void main(String[] args) {
        final int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);
        Controller ignored = new Controller(cport, R, timeout, rebalancePeriod);
    }

    private Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        // Rebalance loop
        Timer timer = new Timer("ServerLoop");
        TimerTask task = new ServerTimerTask(this);
        final Controller main = this;
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
                            HashMap<String, ArrayList<Integer>> loadTries = new HashMap<>();
                            int port = 0;
                            try {
                                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                String line;
                                while ((line = in.readLine()) != null) {
                                    logger.info("Message received: " + line);
                                    String[] splitIn = line.split(" ");
                                    if (splitIn[0].equals(Protocol.JOIN_TOKEN)) {
                                        port = Integer.parseInt(splitIn[1]);
                                        dstores.put(port, client);
                                        fileLocations.put(port, new ArrayList<>());
                                        lastDStore.add(port);
                                        if (dstores.size() >= R && !balancing) {
                                            new Thread(task).start();
                                        }
                                    } else if (splitIn[0].equals(Protocol.LIST_TOKEN)) {
                                        if (port != 0) {
                                            var files = new ArrayList<>(Arrays.asList(splitIn).subList(1, splitIn.length));
                                            fileLocations.remove(port);
                                            fileLocations.put(port, files);
                                            logger.info("Files " + files + " added for " + port);
                                            try {
                                                rebaLatch.countDown();
                                            } catch (NullPointerException e) {
                                            }
                                        } else if (dstores.size() < R) {
                                            sendMsg(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                        } else {
                                            while (balancing) {
                                            }
                                            logger.info("LIST from Client");
                                            var toSend = new StringBuilder(Protocol.LIST_TOKEN);
                                            for (String fileName : index.keySet()) {
                                                if (index.get(fileName) == FileStatusInIndex.STORE_COMPLETE) {
                                                    toSend.append(" ").append(fileName);
                                                }
                                            }
                                            sendMsg(client, toSend.toString());
                                        }
                                    } else {
                                        new Thread(new TextRunnable(port, line, main, client, loadTries) {
                                        }).start();
                                    }
                                }

                            } catch (SocketException e) {
                                logger.info("error " + e.getMessage());
                                if (port != 0) {
                                    dstores.remove(port);
                                    fileLocations.remove(port);
                                    try {
                                        lastDStore.remove(lastDStore.indexOf(port));
                                    } catch (IndexOutOfBoundsException e1) {
                                        logger.info("error " + e.getMessage());
                                    }
                                    logger.info("Removed a Dstore");
                                    if (dstores.isEmpty()) {
                                        index.clear();
                                        fileSizes.clear();
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

    public void textProcessing(String line, int port, Socket client, HashMap<String, ArrayList<Integer>> loadTries) {
        String[] splitIn = line.split(" ");
        String command = splitIn[0];
        switch (command) {
            case Protocol.STORE_TOKEN:
                while (balancing) {
                }
                String fileName = splitIn[1];
                String fileSize = splitIn[2];
                if (dstores.size() < R) {
                    sendMsg(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else if (index.get(fileName) != null) {
                    sendMsg(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    index.put(fileName, FileStatusInIndex.STORE_IN_PROGRESS);
                    fileSizes.put(fileName, fileSize);
                    var toSend = new StringBuilder(Protocol.STORE_TO_TOKEN);

                    Collections.shuffle(lastDStore);
                    try {
                        try {
                            int storeCount = 0;
                            for (var dstorePort : lastDStore) {
                                if (storeCount == R) break;
                                if (!fileLocations.get(dstorePort).contains(fileName) && storeCount < R) {
                                    storeCount += 1;
                                    fileLocations.get(dstorePort).add(fileName);
                                    toSend.append(" ").append(dstorePort);
                                }
                            }
                        } catch (ConcurrentModificationException e) {
                            logger.info("error " + e.getMessage());
                        }
                    } catch (NullPointerException e) {
                        break;
                    }

                    CountDownLatch countDown = new CountDownLatch(R);
                    locksS.put(fileName, countDown);
                    sendMsg(client, toSend.toString());
                    logger.info("Thread paused");
                    try {
                        if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                            index.put(fileName, FileStatusInIndex.STORE_COMPLETE);
                            sendMsg(client, Protocol.STORE_COMPLETE_TOKEN);
                            locksS.remove(fileName);
                            logger.info("Index updated for " + fileName);
                        } else {
                            logger.info("STORE " + fileName + " timeout " + countDown.getCount());
                            index.remove(fileName);
                            locksS.remove(fileName);
                        }
                    } catch (InterruptedException | NullPointerException e) {
                        logger.info("error " + e.getMessage());
                    }
                }
                break;
            case Protocol.STORE_ACK_TOKEN:
                try {
                    locksS.get(splitIn[1]).countDown();
                    logger.info("ACK S " + splitIn[1] + " decremented");
                } catch (NullPointerException e) {
                    logger.info("error " + e.getMessage());
                }
                break;
            case Protocol.REMOVE_ACK_TOKEN:
                try {
                    locksR.get(splitIn[1]).countDown();
                    fileLocations.get(port).remove(splitIn[1]);
                    logger.info("ACK R " + splitIn[1] + " decremented");
                } catch (NullPointerException e) {
                    logger.info("error " + e.getMessage());
                }
                break;
            case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
                try {
                    locksR.get(splitIn[1]).countDown();
                    fileLocations.get(port).remove(splitIn[1]);
                    logger.info("ACK R " + splitIn[1] + " decremented file does not exist");
                } catch (NullPointerException e) {
                    logger.info("error " + e.getMessage());
                }
                break;
            case Protocol.LOAD_TOKEN:
            case Protocol.RELOAD_TOKEN:
                while (balancing) {
                }
                String fileToLoad = splitIn[1];
                var notFoundFlag = true;
                if (dstores.size() >= R) {
                    try {
                        if (index.get(fileToLoad) == FileStatusInIndex.STORE_COMPLETE) {
                            if (splitIn[0].equals(Protocol.LOAD_TOKEN)) {
                                loadTries.put(fileToLoad, new ArrayList<>());
                            }
                            for (Integer store : fileLocations.keySet()) {
                                if (fileLocations.get(store).contains(fileToLoad) && !loadTries.get(fileToLoad).contains(store)) {
                                    sendMsg(client, Protocol.LOAD_FROM_TOKEN + " " + store + " " + fileSizes.get(fileToLoad));
                                    loadTries.get(fileToLoad).add(store);
                                    notFoundFlag = false;
                                    break;
                                }
                            }
                            if (notFoundFlag) {
                                sendMsg(client, Protocol.ERROR_LOAD_TOKEN);
                            }

                        } else {
                            sendMsg(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    } catch (NullPointerException e) {
                        sendMsg(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    }
                } else {
                    sendMsg(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                }
                break;
            case Protocol.REMOVE_TOKEN:
                while (balancing) {
                }
                fileName = splitIn[1];
                if (dstores.size() >= R) {
                    try {
                        if (index.get(fileName) == FileStatusInIndex.STORE_COMPLETE) {
                            index.put(fileName, FileStatusInIndex.REMOVE_IN_PROGRESS);
                            CountDownLatch countDown = new CountDownLatch(R);
                            locksR.put(fileName, countDown);
                            for (Integer p : fileLocations.keySet()) {
                                if (fileLocations.get(p).contains(fileName)) {
                                    sendMsg(dstores.get(p), Protocol.REMOVE_TOKEN + " " + fileName);
                                }
                            }

                            logger.info("Thread paused");
                            try {
                                if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                                    sendMsg(client, Protocol.REMOVE_COMPLETE_TOKEN);
                                    index.remove(fileName);
                                    locksR.remove(fileName);
                                    fileSizes.remove(fileName);
                                    logger.info("Index removed for " + fileName);
                                } else {
                                    logger.info("REMOVE " + fileName + " timeout " + countDown.getCount());
                                    locksR.remove(fileName);
                                }
                            } catch (InterruptedException | NullPointerException e) {
                                logger.info("error " + e.getMessage());
                            }
                        } else {
                            sendMsg(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    } catch (NullPointerException e) {
                        sendMsg(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    }
                } else {
                    sendMsg(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                }
                break;
            case Protocol.REBALANCE_COMPLETE_TOKEN:
                rebaCompLatch.countDown();
                break;
            default:
                logger.info("Malformed message");
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
        if (!index.isEmpty()) {
            while (index.containsValue(FileStatusInIndex.STORE_IN_PROGRESS) || index.containsValue(FileStatusInIndex.REMOVE_IN_PROGRESS)) {
            }
            balancing = true;
            if (dstores.size() >= R) {
                for (Integer s : dstores.keySet()) {
                    sendMsg(dstores.get(s), Protocol.LIST_TOKEN);
                }
                rebaLatch = new CountDownLatch(dstores.size());
                try {
                    if (rebaLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                        double balanceNumber = (double) (R * index.size()) / dstores.size();
                        double floor = Math.floor(balanceNumber);
                        double ceil = Math.ceil(balanceNumber);
                        Hashtable<String, Integer> seen = new Hashtable<>();
                        for (Integer d : fileLocations.keySet()) {
                            for (String f : fileLocations.get(d)) {
                                seen.merge(f, 1, Integer::sum);
                            }
                        }

                        for (Integer d : dstores.keySet()) {
                            HashMap<String, ArrayList<Integer>> send = new HashMap<>();
                            ArrayList<String> toRemove = new ArrayList<>();
                            int count = 0;
                            Collections.shuffle(lastDStore);
                            int dSize = fileLocations.get(d).size();
                            for (String f : fileLocations.get(d)) {
                                if (!index.containsKey(f) && !toRemove.contains(f)) {
                                    toRemove.add(f);
                                } else {
                                    Iterator<Integer> it = lastDStore.iterator();
                                    while (it.hasNext()) {
                                        Integer dSearch = it.next();
                                        if (fileLocations.get(dSearch) != null && !d.equals(dSearch)) {
                                            if (!toRemove.contains(f) && (dSize - toRemove.size()) > floor
                                                    && ((seen.get(f) == R && !fileLocations.get(dSearch).contains(f)
                                                    && fileLocations.get(dSearch).size() < ceil) || seen.get(f) > R)) {
                                                toRemove.add(f);
                                                seen.put(f, seen.get(f) - 1);
                                            }
                                            if (seen.get(f) < R) {
                                                if (!fileLocations.get(dSearch).contains(f)
                                                        && (fileLocations.get(dSearch).size() < ceil || !it.hasNext())) {
                                                    fileLocations.get(dSearch).add(f);
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
                                sendMsg(dstores.get(d), Protocol.REBALANCE_TOKEN + " " + count + message);
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
        if (!balancingL && c.dstores.size() >= c.R) {
            balancingL = true;
            c.rebalance();
            balancingL = false;
        }
    }
}

class TextRunnable implements Runnable {
    private int port;
    private String line;
    private Controller c;
    private Socket client;
    private HashMap<String, ArrayList<Integer>> loadTries;

    TextRunnable(int port, String line, Controller thread, Socket client,
                 HashMap<String, ArrayList<Integer>> loadTries) {
        this.port = port;
        this.line = line;
        this.c = thread;
        this.client = client;
        this.loadTries = loadTries;
    }

    @Override
    public void run() {
        c.textProcessing(line, port, client, loadTries);
    }
}