package org.jgroups.blocks;

import java.io.Closeable;


public interface Connection extends Closeable {
    boolean isOpen();
    boolean isConnected();
    boolean isExpired(long milis);
}