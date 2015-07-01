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
public abstract class ConnectionMap<A extends Address, V extends Connection> {
        
    protected final List<ConnectionMapListener<A,V>> conn_listeners=new ArrayList<>();
    protected final Map<A,V>                         conns=new HashMap<>();
    protected final Lock                             sock_creation_lock=new ReentrantLock(true); // syncs socket establishment
    protected final ThreadFactory                    factory;
    protected long                                   reaperInterval;
    protected final Reaper                           reaper;

    public ConnectionMap(ThreadFactory factory) {
        this(factory,0);        
    }
    
    public ConnectionMap(ThreadFactory factory, long reaperInterval) {
        super();
        this.factory=factory;        
        this.reaperInterval=reaperInterval;
        reaper=reaperInterval > 0?  new Reaper() : null;
    }

    public long getReaperInterval() {return reaperInterval;}
    public ConnectionMap<A,V> setReaperInterval(long interval) {this.reaperInterval=interval; return this;}

    public void start() throws Exception {
        if(reaper != null)
            reaper.start();
    }

    public void stop() {
        if(reaper != null)
            reaper.stop();

        synchronized(this) {
            for(Iterator<Entry<A,V>> i=conns.entrySet().iterator(); i.hasNext(); ) {
                Entry<A,V> e=i.next();
                Util.close(e.getValue());
            }
            conns.clear();
        }
        conn_listeners.clear();
    }


    public synchronized boolean hasConnection(A address) {
        return conns.containsKey(address);
    }

    public synchronized boolean connectionEstablishedTo(A address) {
        V conn=conns.get(address);
        return conn != null && conn.isConnected();
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
    
    public synchronized int getNumConnections() {
        return conns.size();
    }

    public synchronized int getNumOpenConnections() {
        int retval=0;
        for(Connection conn: conns.values())
            if(conn.isOpen())
                retval++;
        return retval;
    }

    public String printConnections() {
        StringBuilder sb=new StringBuilder();
        synchronized(this) {
            for(Map.Entry<A,V> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(A address, V conn) {
        if(address == null || conn == null)
            return;

        synchronized(this) {
            V existing=conns.get(address);
            if(conn == existing) {
                V tmp=conns.remove(address);
                Util.close(tmp);
            }
        }
    }


    /** Removes all connections which are not in current_mbrs */
    public void retainAll(Collection<A> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<A,V> copy=null;
        synchronized(this) {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        }
        copy.keySet().removeAll(current_mbrs);
        for(Map.Entry<A,V> entry: copy.entrySet())
            Util.close(entry.getValue());
        copy.clear();
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
        synchronized(this) {
            for(Map.Entry<A,V> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            return sb.toString();
        }
    }




    protected class Reaper implements Runnable {
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
                synchronized(ConnectionMap.this) {
                    for(Iterator<Entry<A,V>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<A,V> entry=it.next();
                        V c=entry.getValue();
                        if(c.isExpired(System.nanoTime())) {
                            Util.close(c);
                            it.remove();                           
                        }
                    }
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
