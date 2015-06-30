package org.jgroups.blocks;

import java.io.Closeable;


public interface Connection extends Closeable {
    
    boolean isOpen();
    
    boolean isExpired(long milis);

}