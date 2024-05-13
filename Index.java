import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Index {
    public final ConcurrentHashMap<Integer, Socket> dstoreSockets = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Integer, ArrayList<String>> dstoreFileLists = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, FileStatus> fileStatus = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, String> fileSizes = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, CountDownLatch> storeCountDownLatches = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, CountDownLatch> removeCountDownLatches = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, ArrayList<Integer>> fileDownloadHistory = new ConcurrentHashMap<>();

    public Index() {
    }
}
