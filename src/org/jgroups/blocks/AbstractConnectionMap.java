package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class which handles connections and connection reaping
 * @param <A> The type of the address, e.g. {@link Address}
 * @param <V> The type of the connection, e.g. {@link org.jgroups.blocks.TCPConnectionMap.TCPConnection}
 */
public abstract class AbstractConnectionMap<A extends Address, V extends Connection> {
        
    protected final List<ConnectionMapListener<A,V>> conn_listeners=new ArrayList<>();
    protected final Map<A,V>                         conns=new HashMap<>();
    protected final Lock                             lock=new ReentrantLock(); // syncs conns
    protected final Lock                             sock_creation_lock= new ReentrantLock(true); // syncs socket establishment
    protected final ThreadFactory                    factory;
    protected final long                             reaperInterval;
    protected final Reaper                           reaper;

    public AbstractConnectionMap(ThreadFactory factory) {
        this(factory,0);        
    }
    
    public AbstractConnectionMap(ThreadFactory factory,long reaperInterval) {
        super();
        this.factory=factory;        
        this.reaperInterval=reaperInterval;
        reaper=reaperInterval > 0?  new Reaper() : null;
    }
    
    public Lock getLock() {
        return lock;
    }
    

    public boolean hasConnection(A address) {
        lock.lock();
        try {
            return conns.containsKey(address);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean connectionEstablishedTo(A address) {
        lock.lock();
        try {
            V conn=conns.get(address);
            return conn != null && conn.isConnected();
        }
        finally {
            lock.unlock();
        }
    }

    /** Creates a new connection to dest, or returns an existing one */
    public abstract V getConnection(A dest) throws Exception;

    public void addConnection(A address, V conn) {
        V previous = conns.put(address, conn);
        Util.close(previous); // closes previous connection (if present)
        notifyConnectionOpened(address, conn);
    }

    public void addConnectionMapListener(ConnectionMapListener<A,V> cml) {
        if(cml != null && !conn_listeners.contains(cml))
            conn_listeners.add(cml);
    }

    public void removeConnectionMapListener(ConnectionMapListener<A,V> cml) {
        if(cml != null)
            conn_listeners.remove(cml);
    }
    
    public int getNumConnections() {
        lock.lock();
        try {
            return conns.size();
        }
        finally {
            lock.unlock();
        }
    }

    public int getNumOpenConnections() {
        int retval=0;
        lock.lock();
        try {
            for(Connection conn: conns.values())
                if(conn.isOpen())
                    retval++;
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public String printConnections() {
        StringBuilder sb=new StringBuilder();

        lock.lock();
        try {
            for(Map.Entry<A,V> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        finally {
            lock.unlock();
        }
        return sb.toString();
    }

    public ThreadFactory getThreadFactory() {
        return factory;
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(A address, V conn) {
        if(address == null || conn == null)
            return;

        lock.lock();
        try {
            V existing=conns.get(address);
            if(conn == existing) {
                V tmp=conns.remove(address);
                Util.close(tmp);
            }
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes all connections which are not in current_mbrs
     * 
     * @param current_mbrs
     */
    public void retainAll(Collection<A> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<A,V> copy=null;
        lock.lock();
        try {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        }
        finally {
            lock.unlock();
        }   
        copy.keySet().removeAll(current_mbrs);
       
        for(Iterator<Entry<A,V>> i = copy.entrySet().iterator();i.hasNext();) {
            Entry<A,V> e = i.next();
            Util.close(e.getValue());      
        }
        copy.clear();
    }
    
    public void start() throws Exception {
        if(reaper != null)
            reaper.start();
    }

    public void stop() {
        if(reaper != null)
            reaper.stop();

        lock.lock();
        try {
            for(Iterator<Entry<A,V>> i = conns.entrySet().iterator();i.hasNext();) {
                Entry<A,V> e = i.next();
                Util.close(e.getValue());          
            }
            clear();
        }
        finally {
            lock.unlock();
        }           
        conn_listeners.clear();
    }

    protected void clear() {
        lock.lock();
        try {
            conns.clear();
        }
        finally {
            lock.unlock();
        }
    }

    protected void notifyConnectionClosed(A address) {
        for(ConnectionMapListener<A,V> l:conn_listeners)
            l.connectionClosed(address);
    }

    protected void notifyConnectionOpened(A address, V conn) {
        for(ConnectionMapListener<A,V> l:conn_listeners)
            l.connectionOpened(address, conn);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        getLock().lock();
        try {
            for(Map.Entry<A,V> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            return sb.toString();
        }
        finally {
            getLock().unlock();
        }
    }




    class Reaper implements Runnable {
        private Thread thread;

        public synchronized void start() {
            if(thread == null || !thread.isAlive()) {
                thread=factory.newThread(new Reaper(), "Reaper");
                thread.start();
            }
        }

        public synchronized void stop() {
            if(thread != null && thread.isAlive()) {
                thread.interrupt();
                try {
                    thread.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException ignored) {
                }
            }
            thread=null;
        }

        public void run() {
            while(!Thread.currentThread().isInterrupted()) {
                lock.lock();
                try {
                    for(Iterator<Entry<A,V>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<A,V> entry=it.next();
                        V c=entry.getValue();
                        if(c.isExpired(System.nanoTime())) {
                            Util.close(c);
                            it.remove();                           
                        }
                    }
                }
                finally {
                    lock.unlock();
                }
                Util.sleep(reaperInterval);
            }           
        }
    }

    public interface ConnectionMapListener<A extends Address, V extends Connection> {
        void connectionClosed(A address);
        void connectionOpened(A address, V conn);

    }
}
