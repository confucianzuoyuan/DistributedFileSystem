public class RebalanceThread extends Thread {

    private Controller controller;

    public RebalanceThread(Controller controller) {
        this.controller = controller;
    }

    public void run() {
        controller.rebalance();
    }

}