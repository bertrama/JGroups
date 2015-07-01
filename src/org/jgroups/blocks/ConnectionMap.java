package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class which handles connections and connection reaping
 * @param <A> The type of the address, e.g. {@link Address}
 * @param <C> The type of the connection, e.g. {@link org.jgroups.blocks.TCPConnectionMap.TCPConnection}
 */
public abstract class ConnectionMap<A, C extends Connection> {
    protected final List<ConnectionMapListener<A,C>> conn_listeners=new ArrayList<>();
    protected final Map<A,C>                         conns=new HashMap<>();
    protected final Lock                             sock_creation_lock=new ReentrantLock(true); // syncs socket establishment
    protected final ThreadFactory                    factory;
    protected long                                   reaperInterval;
    protected final Reaper                           reaper;
    protected Log                                    log=LogFactory.getLog(getClass());

    public ConnectionMap(ThreadFactory factory) {
        this(factory,0);        
    }
    
    public ConnectionMap(ThreadFactory factory, long reaperInterval) {
        super();
        this.factory=factory;        
        this.reaperInterval=reaperInterval;
        reaper=reaperInterval > 0?  new Reaper() : null;
    }

    public long               getReaperInterval()              {return reaperInterval;}
    public ConnectionMap<A,C> setReaperInterval(long interval) {this.reaperInterval=interval; return this;}
    public ConnectionMap<A,C> log(Log the_log)                 {this.log=the_log; return this;}
    public abstract A         getLocalAddress();


    public void start() throws Exception {
        if(reaper != null)
            reaper.start();
    }

    public void stop() {
        if(reaper != null)
            reaper.stop();

        synchronized(this) {
            for(Map.Entry<A,C> entry: conns.entrySet())
                Util.close(entry.getValue());
            conns.clear();
        }
        conn_listeners.clear();
    }

    /** Creates a new connection object to target dest, but doesn't yet connect it */
    protected abstract C createConnection(A dest) throws Exception;

    public synchronized boolean hasConnection(A address) {
        return conns.containsKey(address);
    }

    public synchronized boolean connectionEstablishedTo(A address) {
        C conn=conns.get(address);
        return conn != null && conn.isConnected();
    }

    /** Creates a new connection to dest, or returns an existing one */
    public C getConnection(A dest) throws Exception {
        C conn;
        synchronized(this) {
            if((conn=conns.get(dest)) != null && conn.isOpen()) // keep FAST path on the most common case
                return conn;
        }

        Exception connect_exception=null; // set if connect() throws an exception
        sock_creation_lock.lockInterruptibly();
        try {
            // lock / release, create new conn under sock_creation_lock, it can be skipped but then it takes
            // extra check in conn map and closing the new connection, w/ sock_creation_lock it looks much simpler
            // (slow path, so not important)

            synchronized(this) {
                conn=conns.get(dest); // check again after obtaining sock_creation_lock
                if(conn != null && conn.isOpen())
                    return conn;

                // create conn stub
                conn=createConnection(dest); // new TCPConnection(dest);
                addConnection(dest, conn);
            }

            // now connect to dest:
            try {
                log.trace("%s: connecting to %s", getLocalAddress(), dest);
                conn.connect(new InetSocketAddress(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort()));
                conn.start();
            }
            catch(Exception connect_ex) {
                connect_exception=connect_ex;
            }

            synchronized(this) {
                C existing_conn=conns.get(dest); // check again after obtaining sock_creation_lock
                if(existing_conn != null && existing_conn.isOpen() // added by a successful accept()
                  && existing_conn != conn) {
                    log.trace("%s: found existing connection to %s, using it and deleting own conn-stub", getLocalAddress(), dest);
                    Util.close(conn); // close our connection; not really needed as conn was closed by accept()
                    return existing_conn;
                }

                if(connect_exception != null) {
                    log.trace("%s: failed connecting to %s: %s", getLocalAddress(), dest, connect_exception);
                    removeConnectionIfPresent(dest, conn); // removes and closes the conn
                    throw connect_exception;
                }
                return conn;
            }
        }
        finally {
            sock_creation_lock.unlock();
        }
    }

    public void addConnection(A address, C conn) {
        C previous = conns.put(address, conn);
        Util.close(previous); // closes previous connection (if present)
        notifyConnectionOpened(address, conn);
    }

    public void addConnectionMapListener(ConnectionMapListener<A,C> cml) {
        if(cml != null && !conn_listeners.contains(cml))
            conn_listeners.add(cml);
    }

    public void removeConnectionMapListener(ConnectionMapListener<A,C> cml) {
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
            for(Map.Entry<A,C> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(A address, C conn) {
        if(address == null || conn == null)
            return;

        synchronized(this) {
            C existing=conns.get(address);
            if(conn == existing) {
                C tmp=conns.remove(address);
                Util.close(tmp);
            }
        }
    }


    /** Removes all connections which are not in current_mbrs */
    public void retainAll(Collection<A> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<A,C> copy=null;
        synchronized(this) {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        }
        copy.keySet().removeAll(current_mbrs);
        for(Map.Entry<A,C> entry: copy.entrySet())
            Util.close(entry.getValue());
        copy.clear();
    }
    

    protected void notifyConnectionClosed(A address) {
        for(ConnectionMapListener<A,C> l:conn_listeners)
            l.connectionClosed(address);
    }

    protected void notifyConnectionOpened(A address, C conn) {
        for(ConnectionMapListener<A,C> l:conn_listeners)
            l.connectionOpened(address, conn);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        synchronized(this) {
            for(Map.Entry<A,C> entry: conns.entrySet())
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
                    for(Iterator<Entry<A,C>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<A,C> entry=it.next();
                        C c=entry.getValue();
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

    public interface ConnectionMapListener<A, V extends Connection> {
        void connectionClosed(A address);
        void connectionOpened(A address, V conn);

    }
}
