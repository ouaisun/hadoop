/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or
 * other DataNodes.  This small server does not use the
 * Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable {

    public static final Logger                    LOG          = DataNode.LOG;
    /// PeerServer是一个接口，实现了它的TcpPeerServer封装饿了一个ServerSocket，提供了Java Socket服务端的功
    private final       PeerServer                peerServer;
    // 该DataXceiverServer所属DataNode实例datanode
    private final       DataNode                  datanode;
    // Peer所在线程的映射集合peers
    private final       HashMap<Peer,Thread>      peers        = new HashMap<Peer,Thread>();
    // Peer与DataXceiver的映射集合peersXceiver
    private final       HashMap<Peer,DataXceiver> peersXceiver = new HashMap<Peer,DataXceiver>();

    // DataXceiverServer是否已关闭的标志位closed
    private boolean closed = false;

    /**
     * Maximal number of concurrent xceivers per node.
     * Enforcing the limit is required in order to avoid data-node
     * running out of memory.
     * * 每个节点并行的最大DataXceivers数目。
     * * 为了避免dataNode运行内存溢出，执行这个限制是必须的。
     * * 定义是默认值为4096.
     */
    int maxXceiverCount =
            DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;

    /**
     * A manager to make sure that cluster balancing does not
     * take too much resources.
     * It limits the number of block moves for balancing and
     * the total amount of bandwidth they can use.
     */
    // 集群数据块平衡节流器balanceThrottler
    static class BlockBalanceThrottler extends DataTransferThrottler {

        // 表示当前移动数据块的线程数numThreads
        private int numThreads;

        // 表示移动数据块的最大线程数maxThreads
        private final AtomicInteger maxThreads = new AtomicInteger(0);

        /**
         * Constructor
         *
         * @param bandwidth Total amount of bandwidth can be used for balancing
         */
        private BlockBalanceThrottler(long bandwidth, int maxThreads) {
            super(bandwidth);
            // 设置移动数据块的最大线程数maxThreads
            this.maxThreads.set(maxThreads);
            LOG.info("Balancing bandwith is " + bandwidth + " bytes/s");
            LOG.info("Number threads for balancing is " + maxThreads);
        }

        private void setMaxConcurrentMovers(int movers) {
            this.maxThreads.set(movers);
        }

        @VisibleForTesting
        int getMaxConcurrentMovers() {
            return this.maxThreads.get();
        }

        /**
         * Check if the block move can start.
         * Return true if the thread quota is not exceeded and
         * the counter is incremented; False otherwise.
         */
        synchronized boolean acquire() {
            if (numThreads >= maxThreads.get()) {
                return false;
            }
            numThreads++;
            return true;
        }

        /** Mark that the move is completed. The thread counter is decremented. */
        synchronized void release() {
            numThreads--;
        }
    }

    final BlockBalanceThrottler balanceThrottler;

    /**
     * We need an estimate for block size to check if the disk partition has
     * enough space. Newer clients pass the expected block size to the DataNode.
     * For older clients we just use the server-side default block size.
     * 我们需要估计块大小以检测磁盘分区是否有足够的空间。
     * * 新客户端传递预期块大小给DataNode。
     * * 对于旧客户端而言我们仅仅使用服务器端默认的块大小
     */
    final long estimateBlockSize;


    DataXceiverServer(PeerServer peerServer, Configuration conf,
                      DataNode datanode) {

        // 根据传入的peerServer设置同名成员变量
        this.peerServer = peerServer;
        // 设置DataNode实例datanode
        this.datanode = datanode;

        // 设置DataNode中DataXceiver的最大数目maxXceiverCount
        // 取参数dfs.datanode.max.transfer.threads，参数未配置的话，默认值为4096
        this.maxXceiverCount =
                conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                        DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);

        // 设置估计块大小estimateBlockSize
        // 取参数dfs.blocksize，参数未配置的话，默认值是128*1024*1024，即128M

        this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
                DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

        //set up parameter for cluster balancing
        //set up parameter for cluster balancing
        // 设置集群平衡节流器
        // 带宽取参数dfs.datanode.balance.bandwidthPerSec，参数未配置默认为1024*1024
        // 最大线程数取参数dfs.datanode.balance.max.concurrent.moves，参数未配置默认为5
        this.balanceThrottler = new BlockBalanceThrottler(
                conf.getLongBytes(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
                        DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT),
                conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
                        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT));
    }

    @Override
    public void run() {
        Peer peer = null;
        // 如果标志位shouldRun为true，且没有为升级而执行shutdown
        while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
            try {
                // 阻塞，直到接收到客户端或者其他DataNode的连接请求
                peer = peerServer.accept();

                // Make sure the xceiver count is not exceeded

                // 确保DataXceiver数目没有超过最大限制
                /**
                 * DataNode的getXceiverCount方法计算得到，返回线程组的活跃线程数目
                 * threadGroup == null ? 0 : threadGroup.activeCount();
                 */
                int curXceiverCount = datanode.getXceiverCount();
                if (curXceiverCount > maxXceiverCount) {
                    throw new IOException("Xceiver count " + curXceiverCount
                            + " exceeds the limit of concurrent xcievers: "
                            + maxXceiverCount);
                }

                // 创建一个后台线程，DataXceiver，并加入到线程组datanode.threadGroup
                new Daemon(datanode.threadGroup,
                        DataXceiver.create(peer, datanode, this))
                        .start();
            } catch (SocketTimeoutException ignored) {
                // wake up to see if should continue to run
            } catch (AsynchronousCloseException ace) {
                // another thread closed our listener socket - that's expected during shutdown,
                // but not in other circumstances
                // 正如我们所预料的，只有在关机的过程中，通过其他线程关闭我们的侦听套接字，其他情况下则不会发生
                if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
                    LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ace);
                }
            } catch (IOException ie) {
                IOUtils.cleanup(null, peer);
                // 数据节点可能由于存在太多的数据传输导致内存溢出，记录该事件，并等待30秒，其他的数据传输可能到时就完成了
                LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ie);
            } catch (OutOfMemoryError ie) {
                IOUtils.cleanup(null, peer);
                // DataNode can run out of memory if there is too many transfers.
                // Log the event, Sleep for 30 seconds, other transfers may complete by
                // then.
                LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
                try {
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            } catch (Throwable te) {
                LOG.error(datanode.getDisplayName()
                        + ":DataXceiverServer: Exiting due to: ", te);
                datanode.shouldRun = false;
            }
        }

        // Close the server to stop reception of more requests.
        // 关闭服务器停止接收更多请求
        try {
            peerServer.close();
            closed = true;
        } catch (IOException ie) {
            LOG.warn(datanode.getDisplayName()
                    + " :DataXceiverServer: close exception", ie);
        }

        // if in restart prep stage, notify peers before closing them.
        // 如果在重新启动前准备阶段，在关闭前通知peers
        if (datanode.shutdownForUpgrade) {
            restartNotifyPeers();
            // Each thread needs some time to process it. If a thread needs
            // to send an OOB message to the client, but blocked on network for
            // long time, we need to force its termination.
            LOG.info("Shutting down DataXceiverServer before restart");
            // Allow roughly up to 2 seconds.
            for (int i = 0; getNumPeers() > 0 && i < 10; i++) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
        // Close all peers.
        // 关闭所有的peers
        closeAllPeers();
    }

    void kill() {
        assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
                "shoudRun should be set to false or restarting should be true"
                        + " before killing";
        try {
            this.peerServer.close();
            this.closed = true;
        } catch (IOException ie) {
            LOG.warn(datanode.getDisplayName() + ":DataXceiverServer.kill(): ", ie);
        }
    }

    synchronized void addPeer(Peer peer, Thread t, DataXceiver xceiver)
            throws IOException {
        if (closed) {
            throw new IOException("Server closed.");
        }
        peers.put(peer, t);
        peersXceiver.put(peer, xceiver);
    }

    synchronized void closePeer(Peer peer) {
        peers.remove(peer);
        peersXceiver.remove(peer);
        IOUtils.cleanup(null, peer);
    }

    // Sending OOB to all peers
    public synchronized void sendOOBToPeers() {
        if (!datanode.shutdownForUpgrade) {
            return;
        }

        for (Peer p : peers.keySet()) {
            try {
                peersXceiver.get(p).sendOOB();
            } catch (IOException e) {
                LOG.warn("Got error when sending OOB message.", e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when sending OOB message.");
            }
        }
    }

    public synchronized void stopWriters() {
        for (Peer p : peers.keySet()) {
            peersXceiver.get(p).stopWriter();
        }
    }

    // Notify all peers of the shutdown and restart.
    // datanode.shouldRun should still be true and datanode.restarting should
    // be set true before calling this method.
    synchronized void restartNotifyPeers() {
        assert (datanode.shouldRun == true && datanode.shutdownForUpgrade);
        for (Thread t : peers.values()) {
            // interrupt each and every DataXceiver thread.
            t.interrupt();
        }
    }

    // Close all peers and clear the map.
    synchronized void closeAllPeers() {
        LOG.info("Closing all peers.");
        for (Peer p : peers.keySet()) {
            IOUtils.cleanup(null, p);
        }
        peers.clear();
        peersXceiver.clear();
    }

    // Return the number of peers.
    synchronized int getNumPeers() {
        return peers.size();
    }

    // Return the number of peers and DataXceivers.
    @VisibleForTesting
    synchronized int getNumPeersXceiver() {
        return peersXceiver.size();
    }

    @VisibleForTesting
    PeerServer getPeerServer() {
        return peerServer;
    }

    synchronized void releasePeer(Peer peer) {
        peers.remove(peer);
        peersXceiver.remove(peer);
    }

    public void updateBalancerMaxConcurrentMovers(int movers) {
        balanceThrottler.setMaxConcurrentMovers(movers);
    }
}
