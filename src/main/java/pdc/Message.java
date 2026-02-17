package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public byte type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
       
    try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);

        int payloadLength = (payload == null) ? 0 : payload.length;

        // ---- Write header ----
        dos.writeInt(0); // placeholder for total length
        dos.writeInt(magicBytes.length);
        dos.write(magicBytes);
        dos.writeInt(version);
        dos.writeByte(type);
        dos.writeInt(senderBytes.length);
        dos.write(senderBytes);
        dos.writeLong(timestamp);
        dos.writeInt(payloadLength);

        if (payloadLength > 0) {
            dos.write(payload);
        }

        dos.flush();

        byte[] messageBytes = baos.toByteArray();

        // Now update total length at beginning
        ByteArrayOutputStream finalBaos = new ByteArrayOutputStream();
        DataOutputStream finalDos = new DataOutputStream(finalBaos);

        finalDos.writeInt(messageBytes.length);
        finalDos.write(messageBytes, 4, messageBytes.length - 4);

        finalDos.flush();
        return finalBaos.toByteArray();

    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}

    

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(DataInputStream dis) throws IOException {

    dis.readInt(); // totallength (ignored)

    int magicLength = dis.readInt();
    byte[] magicBytes = new byte[magicLength];
    dis.readFully(magicBytes);

    int version = dis.readInt();
    byte type = dis.readByte();

    int senderLength = dis.readInt();
    byte[] senderBytes = new byte[senderLength];
    dis.readFully(senderBytes);

    long timestamp = dis.readLong();

    int payloadLength = dis.readInt();
    byte[] payload = new byte[payloadLength];
    if (payloadLength > 0) {
        dis.readFully(payload);
    }

    Message msg = new Message();
    msg.magic = new String(magicBytes, StandardCharsets.UTF_8);
    msg.version = version;
    msg.type = type;
    msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
    msg.timestamp = timestamp;
    msg.payload = payload;

    return msg;
}
 
}
