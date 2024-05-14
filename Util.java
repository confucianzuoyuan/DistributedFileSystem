import java.io.PrintWriter;
import java.net.Socket;

public class Util {
    public static void sendMessage(Socket socket, String msg) {
        try {
            var out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
