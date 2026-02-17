package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */


public class Master {
private final ConcurrentHashMap<String, Long> heartbeats = new ConcurrentHashMap<>();

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
       
    if (data == null || data.length == 0) {
        return null;
    }

    int rows = data.length;
    int cols = data[0].length;

    int[][] result = new int[rows][cols];

    // Basic placeholder computation (sequential for now)
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            result[i][j] = data[i][j]; // temporary
        }
    }

    return result;
}

        
    


    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */

    

    public void listen(int port) throws IOException {
    ServerSocket serverSocket = new ServerSocket(port);

    while (true) {
        Socket socket = serverSocket.accept();

        systemThreads.submit(() -> {
            try {
                handleWorker(socket);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}



    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    private void handleWorker(Socket socket) throws Exception {
    DataInputStream dis = new DataInputStream(socket.getInputStream());
    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

    while (true) {
        Message msg = Message.unpack(dis);

        switch (msg.type) {
            case 1: 
                System.out.println("Worker registered");
                break;
            case 2:
                System.out.println("Result received");
                break;
            case 3: 
                System.out.println("Heartbeat received");
                break;
        }
    }
}

    public void reconcileState() {
        long now = System.currentTimeMillis();
        
    heartbeats.forEach((workerId, lastBeat) -> {
        if (now - lastBeat > 5000) {
            System.out.println("Worker failed: " + workerId);
            // reassign task
        }
    });

    }
}
