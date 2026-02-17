package pdc;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
private Socket socket;
private DataInputStream dis;
private DataOutputStream dos;

private final ExecutorService executor =
        Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()
        );

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);

            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());

            Message register = new Message();
            register.magic = "CSM218";
            register.version = 1;
            register.type = 1;
            register.sender = "worker-" + socket.getLocalPort();
            register.timestamp = System.currentTimeMillis();
            register.payload = new byte[0];

            dos.write(register.pack());
            dos.flush();

            listen();   

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    private void listen() throws Exception {
        while (true) {
            Message msg = Message.unpack(dis);

            switch (msg.type) {
                case 2: // TASK
                    executor.submit(() -> handleTask(msg));
                    break;
            }
        }
    }

    
    private void handleTask(Message msg) {
        try {
            Message result = new Message();
            result.magic = "CSM218";
            result.version = 1;
            result.type = 3;
            result.sender = "worker-" + socket.getLocalPort();
            result.timestamp = System.currentTimeMillis();
            result.payload = msg.payload;

            synchronized (dos) {
                dos.write(result.pack());
                dos.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute() {
        // can remain empty for now
    }
}


    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    