package org.jgroups.nio;

import java.nio.ByteBuffer;

/**
 * NIO based server interface for sending byte[] buffers to a single or all members and for receiving byte[] buffers.
 * Implementations should be able to be used to provide light weight NIO servers.
 * @param <T> The type of the address, e.g. {@link org.jgroups.Address}
 * @author Bela Ban
 * @since  3.6.5
 */

public interface Server<T> {

    /**
     * Sets a receiver
     * @param receiver The receiver
     */
    void setReceiver(Receiver<T> receiver);

    /**
     * Starts the server. Implementations will typically create a socket and start a select loop. Note that start()
     * should return immediately, and work (e.g. select()) should be done in a separate thread
     */
    void start();

    /** Stops the server, e.g. by killing the thread calling select() */
    void stop();

    /**
     * Sends a message to a destination
     * @param dest The destination. If null, the message should be sent to all members
     * @param buf The buffer
     * @param offset The offset into the buffer
     * @param length The number of bytes to be sent
     */
    void send(T dest, byte[] buf, int offset, int length);

    /**
     * Sends a message to a destination
     * @param dest The destination. If null, the message should be sent to all members
     * @param buf The buffer to be sent
     */
    void send(T dest, ByteBuffer buf);
}
