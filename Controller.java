import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {
    private static int controllerPort;
    private static int replicaNumber;
    private static int timeout;
    private static int rebalancePeriod;
    private static ConcurrentHashMap<Integer, Socket> dstoreMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, FileInfo> fileInfoMap = new ConcurrentHashMap<>();

    private static boolean isRebalancing = false;

    private static CountDownLatch waitForAllDstoresListCommand;
    private static CountDownLatch oneDstoreCompleteRebalance;

    public static void rebalance() {
        var t1 = System.currentTimeMillis();
        isRebalancing = true;
        if (dstoreMap.size() >= replicaNumber) {
            System.out.println("enter rebalance");
            /// when file is storing or removing, halt
            for (var fileName : fileInfoMap.keySet()) {
                var fileInfo = fileInfoMap.get(fileName);
                /// 如果文件正在存储或者删除，则halt
                while (fileInfo.status == FileStatus.STORE_IN_PROGRESS || fileInfo.status == FileStatus.REMOVE_IN_PROGRESS) {
                }
            }

            for (var dstorePort : dstoreMap.keySet()) {
                System.out.println("send list to dstore: " + dstorePort);
                Util.sendMessage(dstoreMap.get(dstorePort), Protocol.LIST_TOKEN);
            }

            // 等待所有dstore发来LIST FILES
            waitForAllDstoresListCommand = new CountDownLatch(dstoreMap.size());
            try {
                if (waitForAllDstoresListCommand.await(timeout, TimeUnit.MILLISECONDS)) {
                    var filesNumberInEveryDstore = (double) (replicaNumber * fileInfoMap.size()) / dstoreMap.size();
                    var low = Math.floor(filesNumberInEveryDstore);
                    var high = Math.ceil(filesNumberInEveryDstore);

                    // calculate file number in every dstore now
                    // 计算每个dstore保存的文件列表
                    var filesInDstore = new HashMap<Integer, HashSet<String>>();
                    for (var fileName : fileInfoMap.keySet()) {
                        var fileInfo = fileInfoMap.get(fileName);
                        for (var dp : fileInfo.dstoresSavingFiles) {
                            if (!filesInDstore.containsKey(dp)) {
                                filesInDstore.put(dp, new HashSet<>());
                            }
                            filesInDstore.get(dp).add(fileName);
                        }
                    }

                    /// 遍历每一个dstore
                    for (var dstore : dstoreMap.keySet()) {
                        /// 用来保存从这个dstore要发送出去的文件，以及目的地
                        var filesToSendToDstore = new HashMap<String, HashSet<Integer>>();
                        /// 用来保存从这个dstore要删除的文件
                        var filesToRemoveInDstore = new HashSet<String>();

                        /// 获取当前dstore保存的文件列表
                        var files = filesInDstore.get(dstore);
                        if (files == null) continue;
                        /// 遍历文件列表
                        for (var file : files) {
                            /// case 1: file is not STORE_COMPLETE
                            /// 如果文件不是保存成功的，加入删除列表
                            /// case 2: file numbers > replicaNumber
                            /// 如果文件数量超过了数量
                            if (fileInfoMap.get(file).status != FileStatus.STORE_COMPLETE
                                    || fileInfoMap.get(file).dstoresSavingFiles.size() > replicaNumber) {
                                filesToRemoveInDstore.add(file);
                                fileInfoMap.get(file).dstoresSavingFiles.remove(dstore);
                                filesInDstore.get(dstore).remove(file);
                                continue;
                            }

                            /// case 3: file number in dstore - file to remove now > high
                            /// 文件的数量本身已经等于replicaNumber
                            /// 当前dstore中存储的文件数量 - 要删除的文件数量 如果大于 应该存储文件数量的上限，就应该将文件挪走了
                            if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() > high
                                    && fileInfoMap.get(file).dstoresSavingFiles.size() == replicaNumber) {
                                filesToRemoveInDstore.add(file);
                                fileInfoMap.get(file).dstoresSavingFiles.remove(dstore);
                                filesInDstore.get(dstore).remove(file);
                                /// send this file to another dstore
                                /// 挑出要把文件发送到哪个dstore中
                                var anotherDstoreList = dstoreMap.keySet().iterator();
                                while (anotherDstoreList.hasNext()) {
                                    var anotherDstore = anotherDstoreList.next();
                                    if (anotherDstore.equals(dstore)) continue;
                                    /// another dstore contain the file
                                    /// 如果另一个dstore也包含这个文件，不选择
                                    if (filesInDstore.get(anotherDstore).contains(file)) continue;
                                    /// another dstore is full
                                    /// 如果另一个dstore已经满了，也不选择
                                    if (filesInDstore.get(anotherDstore).size() > low) continue;
                                    /// 添加到发送目的地
                                    if (!filesToSendToDstore.containsKey(file)) {
                                        filesToSendToDstore.put(file, new HashSet<>());
                                    }
                                    filesToSendToDstore.get(file).add(anotherDstore);
                                    fileInfoMap.get(file).dstoresSavingFiles.add(anotherDstore);
                                    filesInDstore.get(anotherDstore).add(file);
                                    break;
                                }
                                continue;
                            }

                            /// case 4: 文件的数量小于replicaNumber且dstore超量了
                            if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() > high
                                    && fileInfoMap.get(file).dstoresSavingFiles.size() < replicaNumber) {
                                filesToRemoveInDstore.add(file);
                                fileInfoMap.get(file).dstoresSavingFiles.remove(dstore);
                                filesInDstore.get(dstore).remove(file);
                                /// send this file to another dstore
                                /// 挑出要把文件发送到哪个dstore中
                                var anotherDstoreList = dstoreMap.keySet().iterator();
                                while (anotherDstoreList.hasNext()) {
                                    var anotherDstore = anotherDstoreList.next();
                                    if (anotherDstore.equals(dstore)) continue;
                                    /// another dstore contain the file
                                    /// 如果另一个dstore也包含这个文件，不选择
                                    if (filesInDstore.get(anotherDstore).contains(file)) continue;
                                    /// another dstore is full
                                    /// 如果另一个dstore已经满了，也不选择
                                    if (filesInDstore.get(anotherDstore).size() > low) continue;
                                    /// 添加到发送目的地
                                    if (!filesToSendToDstore.containsKey(file)) {
                                        filesToSendToDstore.put(file, new HashSet<>());
                                    }
                                    filesToSendToDstore.get(file).add(anotherDstore);
                                    fileInfoMap.get(file).dstoresSavingFiles.add(anotherDstore);
                                    filesInDstore.get(anotherDstore).add(file);
                                    if (fileInfoMap.get(file).dstoresSavingFiles.size() == replicaNumber) {
                                        break;
                                    }
                                }
                                continue;
                            }

                            /// case 5: 文件的数量小于replicaNumber且dstore没超量
                            if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() <= high
                                    && fileInfoMap.get(file).dstoresSavingFiles.size() < replicaNumber) {
                                /// send this file to another dstore
                                /// 挑出要把文件发送到哪个dstore中
                                var anotherDstoreList = dstoreMap.keySet().iterator();
                                while (anotherDstoreList.hasNext()) {
                                    var anotherDstore = anotherDstoreList.next();
                                    if (anotherDstore.equals(dstore)) continue;
                                    /// another dstore contain the file
                                    /// 如果另一个dstore也包含这个文件，不选择
                                    if (filesInDstore.get(anotherDstore).contains(file)) continue;
                                    /// another dstore is full
                                    /// 如果另一个dstore已经满了，也不选择
                                    if (filesInDstore.get(anotherDstore).size() > low) continue;
                                    /// 添加到发送目的地
                                    if (!filesToSendToDstore.containsKey(file)) {
                                        filesToSendToDstore.put(file, new HashSet<>());
                                    }
                                    filesToSendToDstore.get(file).add(anotherDstore);
                                    fileInfoMap.get(file).dstoresSavingFiles.add(anotherDstore);
                                    filesInDstore.get(anotherDstore).add(file);
                                    if (fileInfoMap.get(file).dstoresSavingFiles.size() == replicaNumber) {
                                        break;
                                    }
                                }
                            }
                        }

                        /// REBALANCE 2 f1 2 p1 p2 f2 1 p3 2 f2 f3
                        var message = new StringBuilder(Protocol.REBALANCE_TOKEN);
                        message.append(" ").append(filesToSendToDstore.size());
                        for (var file : filesToSendToDstore.keySet()) {
                            message.append(" ").append(file);
                            message.append(" ").append(filesToSendToDstore.get(file).size());
                            for (var ds : filesToSendToDstore.get(file)) {
                                message.append(" ").append(ds);
                            }
                        }

                        message.append(" ").append(filesToRemoveInDstore.size());
                        for (var fileToRemove : filesToRemoveInDstore) {
                            message.append(" ").append(fileToRemove);
                        }

                        if (!filesToSendToDstore.isEmpty() || !filesToRemoveInDstore.isEmpty()) {
                            oneDstoreCompleteRebalance = new CountDownLatch(1);
                            Util.sendMessage(dstoreMap.get(dstore), message.toString());
                            var ignored = oneDstoreCompleteRebalance.await(timeout, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        isRebalancing = false;
        var t2 = System.currentTimeMillis();
        System.out.println("rebalance time: " + (t2 - t1) / 1000);
    }

    public static void main(String[] args) {
        controllerPort = Integer.parseInt(args[0]);
        replicaNumber = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalancePeriod = Integer.parseInt(args[3]);

        var rebalanceTask = new TimerTask() {
            @Override
            public void run() {
                if (!isRebalancing) rebalance();
            }
        };

        new Timer("rebalance task").schedule(rebalanceTask, rebalancePeriod * 1000L, rebalancePeriod * 1000L);

        try (var serverSocket = new ServerSocket(controllerPort)) {
            while (true) {
                var socket = serverSocket.accept();
                var in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    var tokens = inputLine.split(" ");
                    if (tokens[0].equals(Protocol.JOIN_TOKEN)) {
                        System.out.println("Dstore " + tokens[1] + " connect");
                        int dstorePort = Integer.parseInt(tokens[1]);
                        /// add dstore info to dstoreMap
                        dstoreMap.put(dstorePort, socket);
                        new Thread(new DstoreHandler(dstorePort, socket)).start();
                        if (!isRebalancing) rebalance();
                        break;
                    } else {
                        var line = inputLine;
                        new Thread(() -> handleCommandFromClient(socket, line)).start();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void handleCommandFromClient(Socket client, String command) {
        var tokens = command.split(" ");
        switch (tokens[0]) {
            case Protocol.LIST_TOKEN -> {
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else {
                    System.out.println("LIST Start");
                    var t1 = System.currentTimeMillis();
                    while (isRebalancing) {
                    }
                    System.out.println("LIST End");
                    var t2 = System.currentTimeMillis();
                    System.out.println("list time: " + (t2 - t1) / 1000);
                    var message = new StringBuilder(Protocol.LIST_TOKEN);
                    for (var file : fileInfoMap.keySet()) {
                        var fileInfo = fileInfoMap.get(file);
                        if (fileInfo.status == FileStatus.STORE_COMPLETE) {
                            message.append(" ").append(file);
                        }
                    }
                    Util.sendMessage(client, message.toString());
                }
            }
            case Protocol.STORE_TOKEN -> {
                System.out.println("Store start");
                var t1 = System.currentTimeMillis();
                while (isRebalancing) {
                }
                System.out.println("Store end");
                var t2 = System.currentTimeMillis();
                System.out.println("await time: " + (t2 - t1) / 1000L);

                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else if (fileInfoMap.containsKey(tokens[1])) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    var fileName = tokens[1];
                    var fileSize = tokens[2];
                    fileInfoMap.put(fileName, new FileInfo(fileSize));
                    var fileInfo = fileInfoMap.get(fileName);

                    /// select replicaNumber dstores that did not save file for saving file
                    var message = new StringBuilder(Protocol.STORE_TO_TOKEN);
                    int storeCount = 0;
                    for (var dstorePort : dstoreMap.keySet()) {
                        if (storeCount == replicaNumber) break;
                        if (!fileInfo.dstoresSavingFiles.contains(dstorePort)) {
                            storeCount++;
                            fileInfo.dstoresSavingFiles.add(dstorePort);
                            message.append(" ").append(dstorePort);
                        }
                    }

                    /// wait for all dstore's store_ack that the dstore will save file

                    fileInfo.storeLatch = new CountDownLatch(replicaNumber);
                    Util.sendMessage(client, message.toString());
                    try {
                        if (fileInfo.storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                            // update file info with store complete
                            fileInfo.status = FileStatus.STORE_COMPLETE;
                            // send message to client
                            Util.sendMessage(client, Protocol.STORE_COMPLETE_TOKEN);
                            fileInfo.storeLatch = null;
                        } else {
                            // save failed, remove the fileInfo
                            fileInfo.storeLatch = null;
                            fileInfo.status = FileStatus.STORE_FAILED;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            case Protocol.REMOVE_TOKEN -> {
                System.out.println("line: " + fileInfoMap.containsKey(tokens[1]));
                while (isRebalancing) {}
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else if (!fileInfoMap.containsKey(tokens[1])) {
                    System.out.println("file does not exist");
                    Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    var fileName = tokens[1];
                    var fileInfo = fileInfoMap.get(fileName);
                    fileInfo.status = FileStatus.REMOVE_IN_PROGRESS;
                    fileInfo.removeLatch = new CountDownLatch(replicaNumber);
                    for (var dstorePort : fileInfo.dstoresSavingFiles) {
                        fileInfo.dstoresSavingFiles.remove(dstorePort);
                        Util.sendMessage(dstoreMap.get(dstorePort), Protocol.REMOVE_TOKEN + " " + fileName);
                    }

                    try {
                        if (fileInfo.removeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                            Util.sendMessage(client, Protocol.REMOVE_COMPLETE_TOKEN);
                            fileInfoMap.remove(fileName);
                        } else {
                            fileInfo.removeLatch = null;
                            fileInfo.status = FileStatus.REMOVE_FAILED;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN -> {
                while (isRebalancing) {}
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else if (!fileInfoMap.containsKey(tokens[1])) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    var fileName = tokens[1];
                    var fileInfo = fileInfoMap.get(fileName);
                    System.out.println(fileInfo);
                    if (fileInfo.status == FileStatus.STORE_COMPLETE) {
                        if (tokens[0].equals(Protocol.LOAD_TOKEN)) {
                            fileInfo.loadHistory = new HashSet<>();
                        }
                        FileInfo fileFound = null;
                        for (var dstorePort : fileInfo.dstoresSavingFiles) {
                            if (!fileInfo.loadHistory.contains(dstorePort)) {
                                Util.sendMessage(client, Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileInfo.size);
                                fileInfo.loadHistory.add(dstorePort);
                                fileFound = fileInfo;
                                break;
                            }
                        }
                        if (fileFound == null) Util.sendMessage(client, Protocol.ERROR_LOAD_TOKEN);
                    } else {
                        Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    }
                }
            }
        }
    }

    public static class DstoreHandler implements Runnable {
        private int dstorePort;
        private Socket dstoreSocket;

        public DstoreHandler(int dstorePort, Socket dstoreSocket) {
            this.dstorePort = dstorePort;
            this.dstoreSocket = dstoreSocket;
        }

        @Override
        public void run() {
            try {
                var in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    var tokens = inputLine.split(" ");
                    switch (tokens[0]) {
                        case Protocol.LIST_TOKEN -> {
                            /// because of .await, use virtual thread
                            Thread.ofVirtual().start(() -> {
                                var files = new ArrayList<>(Arrays.asList(tokens).subList(1, tokens.length));
                                for (var file : files) {
                                    if (fileInfoMap.containsKey(file)) {
                                        var fileInfo = fileInfoMap.get(file);
                                        fileInfo.dstoresSavingFiles.add(dstorePort);
                                    }
                                }
                                waitForAllDstoresListCommand.countDown();
                            });
                        }
                        case Protocol.STORE_ACK_TOKEN -> {
                            var fileName = tokens[1];
                            var fileInfo = fileInfoMap.get(fileName);
                            fileInfo.storeLatch.countDown();
                        }
                        case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                            var fileName = tokens[1];
                            var fileInfo = fileInfoMap.get(fileName);
                            fileInfo.removeLatch.countDown();
                            fileInfo.dstoresSavingFiles.remove(dstorePort);
                        }
                        case Protocol.REBALANCE_COMPLETE_TOKEN -> {
                            oneDstoreCompleteRebalance.countDown();
                        }
                    }
                }
                /// when dstore disconnect, come here
                dstoreMap.remove(dstorePort);
                for (var file : fileInfoMap.keySet()) {
                    var fileInfo = fileInfoMap.get(file);
                    fileInfo.loadHistory.remove(dstorePort);
                    fileInfo.dstoresSavingFiles.remove(dstorePort);
                }
                try {
                    System.out.println("close disconnected dstore socket");
                    dstoreSocket.close();
                } catch (IOException closeError) {
                    closeError.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FileInfo {
        public String size;
        public FileStatus status;
        // save the file is loading from which dstores
        public HashSet<Integer> loadHistory;
        public HashSet<Integer> dstoresSavingFiles;
        public CountDownLatch storeLatch;
        public CountDownLatch removeLatch;

        public FileInfo(String size) {
            this.size = size;
            this.status = FileStatus.STORE_IN_PROGRESS;
            this.loadHistory = new HashSet<>();
            this.dstoresSavingFiles = new HashSet<>();
            this.storeLatch = null;
            this.removeLatch = null;
        }

        @Override
        public String toString() {
            var s = new StringBuilder();
            s.append("dstoresSavingFiles: ");
            for (var ds : dstoresSavingFiles) {
                s.append(" ").append(ds);
            }
            s.append("   loadHistory: ");
            for (var ds : loadHistory) {
                s.append(" ").append(ds);
            }
            return s.toString();
        }
    }
}