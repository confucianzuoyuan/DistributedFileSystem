import java.util.ArrayList;

public class StoreRequest {

    private ControllerClientHandler clientEndpoint;
    private ArrayList<Integer> dstorePorts;

    public StoreRequest(ControllerClientHandler clientEndpoint, ArrayList<Integer> dstorePorts){
        this.clientEndpoint = clientEndpoint;
        this.dstorePorts = dstorePorts;
    }

    public ControllerClientHandler getClientEndpoint(){
        return clientEndpoint;
    }

    public ArrayList<Integer> getDstorePorts(){
        return dstorePorts;
    }
    
}
