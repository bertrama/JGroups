
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.blocks.TCPConnectionMap;
import org.jgroups.util.SocketFactory;

import java.util.Collection;

/**
 * TCP based protocol. Creates a server socket, which gives us the local address
 * of this group member. For each accept() on the server socket, a new thread is
 * created that listens on the socket. For each outgoing message m, if m.dest is
 * in the outgoing hash table, the associated socket will be reused to send
 * message, otherwise a new socket is created and put in the hash table. When a
 * socket connection breaks or a member is removed from the group, the
 * corresponding items in the incoming and outgoing hash tables will be removed
 * as well.
 * <p>
 * 
 * This functionality is in TCPConnectionMap, which is used by TCP. TCP sends
 * messages using ct.send() and registers with the connection table to receive
 * all incoming messages.
 * 
 * @author Bela Ban
 */
public class TCP extends BasicTCP implements TCPConnectionMap.Receiver {
    
    private TCPConnectionMap ct=null;

    public TCP() {}


    @ManagedAttribute
    public int getOpenConnections() {
        return ct.getNumConnections();
    }

    @ManagedOperation
    public String printConnections() {
        return ct.printConnections();
    }

    public void setSocketFactory(SocketFactory factory) {
        super.setSocketFactory(factory);
        if(ct != null)
            ct.setSocketFactory(factory);
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(ct != null)
            ct.send(dest, data, offset, length);
    }

    public void retainAll(Collection<Address> members) {
        ct.retainAll(members);
    }

    public void start() throws Exception {
        ct=new TCPConnectionMap("jgroups.tcp.srv_sock", getThreadFactory(), getSocketFactory(), this,
                                bind_addr, external_addr, external_port, bind_port, bind_port+port_range, reaper_interval, conn_expire_time)
          .log(log).timeService(time_service)
          .setReceiveBufferSize(recv_buf_size).setSendQueueSize(send_queue_size)
          .setUseSendQueues(use_send_queues).setSendBufferSize(send_buf_size)
          .setSocketConnectionTimeout(sock_conn_timeout)
          .peerAddressReadTimeout(peer_addr_read_timeout)
          .setTcpNodelay(tcp_nodelay).setLinger(linger)
          .setSocketFactory(getSocketFactory())
          .clientBindAddress(client_bind_addr).clientBindPort(client_bind_port).deferClientBinding(defer_client_bind_addr);

        if(reaper_interval > 0 || conn_expire_time > 0) {
            if(reaper_interval == 0) {
                reaper_interval=5000;
                log.warn("reaper_interval was 0, set it to %d", reaper_interval);
            }
            if(conn_expire_time == 0) {
                conn_expire_time=1000 * 60 * 5;
                log.warn("conn_expire_time was 0, set it to %d", conn_expire_time);
            }
            ct.setConnExpireTimeout(conn_expire_time).setReaperInterval(reaper_interval);
        }

        // we first start threads in TP (http://jira.jboss.com/jira/browse/JGRP-626)
        super.start();
    }
    
    public void stop() {
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        ct.stop(); //not needed, but just in case
        super.stop();
    }


    protected void handleConnect() throws Exception {
        if(isSingleton()) {
            if(connect_count == 0) {
                ct.start();
            }
            super.handleConnect();
        }
        else
            ct.start();
    }

    protected void handleDisconnect() {
        if(isSingleton()) {
            super.handleDisconnect();
            if(connect_count == 0)
                ct.stop();
        }
        else
            ct.stop();
    }   



    protected PhysicalAddress getPhysicalAddress() {
        return ct != null? (PhysicalAddress)ct.getLocalAddress() : null;
    }
}
