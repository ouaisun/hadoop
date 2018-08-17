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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
@InterfaceAudience.Private
class BlockPoolManager {

    private static final Logger LOG = DataNode.LOG;

    // NameserviceId与BPOfferService的对应关系
    private final Map<String,BPOfferService> bpByNameserviceId = Maps.newHashMap();
    // BlockPoolId与BPOfferService的对应关系
    private final Map<String,BPOfferService> bpByBlockPoolId   = Maps.newHashMap();
    // 所有的BPOfferService
    private final List<BPOfferService>       offerServices     = new CopyOnWriteArrayList<>();

    // DataNode实例dn
    private final DataNode dn;

    //This lock is used only to ensure exclusion of refreshNamenodes
    // 这个refreshNamenodesLock仅仅在refreshNamenodes()方法中被用作互斥锁
    private final Object refreshNamenodesLock = new Object();

    BlockPoolManager(DataNode dn) {
        this.dn = dn;
    }

    synchronized void addBlockPool(BPOfferService bpos) {
        Preconditions.checkArgument(offerServices.contains(bpos),
                "Unknown BPOS: %s", bpos);
        if (bpos.getBlockPoolId() == null) {
            throw new IllegalArgumentException("Null blockpool id");
        }
        bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
    }

    /**
     * Returns a list of BPOfferService objects. The underlying list
     * implementation is a CopyOnWriteArrayList so it can be safely
     * iterated while BPOfferServices are being added or removed.
     * Caution: The BPOfferService returned could be shutdown any time.
     */
    synchronized List<BPOfferService> getAllNamenodeThreads() {
        return Collections.unmodifiableList(offerServices);
    }

    synchronized BPOfferService get(String bpid) {
        return bpByBlockPoolId.get(bpid);
    }

    synchronized void remove(BPOfferService t) {
        offerServices.remove(t);
        if (t.hasBlockPoolId()) {
            // It's possible that the block pool never successfully registered
            // with any NN, so it was never added it to this map
            bpByBlockPoolId.remove(t.getBlockPoolId());
        }

        boolean removed = false;
        for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
             it.hasNext() && !removed; ) {
            BPOfferService bpos = it.next();
            if (bpos == t) {
                it.remove();
                LOG.info("Removed " + bpos);
                removed = true;
            }
        }

        if (!removed) {
            LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
        }
    }

    void shutDownAll(List<BPOfferService> bposList) throws InterruptedException {
        for (BPOfferService bpos : bposList) {
            bpos.stop(); //interrupts the threads
        }
        //now join
        for (BPOfferService bpos : bposList) {
            bpos.join();
        }
    }

    synchronized void startAll() throws IOException {
        try {
            UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            for (BPOfferService bpos : offerServices) {
                                bpos.start();
                            }
                            return null;
                        }
                    });
        } catch (InterruptedException ex) {
            IOException ioe = new IOException();
            ioe.initCause(ex.getCause());
            throw ioe;
        }
    }

    void joinAll() {
        for (BPOfferService bpos : this.getAllNamenodeThreads()) {
            bpos.join();
        }
    }

    void refreshNamenodes(Configuration conf)
            throws IOException {
        LOG.info("Refresh request received for nameservices: " + conf.get
                (DFSConfigKeys.DFS_NAMESERVICES));


        // 从配置信息conf中获取nameserviceid->{namenode名称->InetSocketAddress}的映射集合newAddressMap
        Map<String,Map<String,InetSocketAddress>> newAddressMap = DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        Map<String,Map<String,InetSocketAddress>> newLifelineAddressMap = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // 需要通过使用synchronized关键字在refreshNamenodesLock上加互斥锁
        synchronized (refreshNamenodesLock) {
            // 调用doRefreshNamenodes()方法执行集合newAddressMap中的刷新
            doRefreshNamenodes(newAddressMap, newLifelineAddressMap);
        }
    }

    private void doRefreshNamenodes(
            Map<String,Map<String,InetSocketAddress>> addrMap,
            Map<String,Map<String,InetSocketAddress>> lifelineAddrMap)
            throws IOException {
        // 确保当前线程在refreshNamenodesLock上拥有互斥锁
        assert Thread.holdsLock(refreshNamenodesLock);

        // 定义三个集合，分别为待刷新的toRefresh、待添加的toAdd和待移除的toRemove
        Set<String> toRefresh = Sets.newLinkedHashSet();
        Set<String> toAdd = Sets.newLinkedHashSet();
        Set<String> toRemove;

        // 使用synchronized关键字在当前对象上获得互斥锁
        synchronized (this) {
            // Step 1. For each of the new nameservices, figure out whether
            // it's an update of the set of NNs for an existing NS,
            // or an entirely new nameservice.
            // 第一步，针对所有新的nameservices中的每个nameservice，
            // 确认它是一个已存在nameservice中的被更新了的NN集合，还是完全的一个新的nameservice
            // 判断的依据就是对应nameserviceId是否在bpByNameserviceId结合中存在

            // 循环addrMap，放入待添加或者待刷新集合
            for (String nameserviceId : addrMap.keySet()) {

                // 如果bpByNameserviceId结合中存在nameserviceId，加入待刷新集合toRefresh，否则加入到待添加集合toAdd
                if (bpByNameserviceId.containsKey(nameserviceId)) {
                    toRefresh.add(nameserviceId);
                } else {
                    toAdd.add(nameserviceId);
                }
            }

            // Step 2. Any nameservices we currently have but are no longer present
            // need to be removed.
            // 第二步，删除所有我们目前拥有但是现在不再需要的，也就是bpByNameserviceId中存在，而配置信息addrMap中没有的

            // 加入到待删除集合toRemove
            toRemove = Sets.newHashSet(Sets.difference(bpByNameserviceId.keySet(), addrMap.keySet()));

            // 验证，待刷新集合toRefresh的大小与待添加集合toAdd的大小必须等于配置信息addrMap中的大小
            assert toRefresh.size() + toAdd.size() ==
                    addrMap.size() :
                    "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
                            "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
                            "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);


            // Step 3. Start new nameservices
            // 第三步，启动所有新的nameservices
            if (!toAdd.isEmpty()) {
                LOG.info("Starting BPOfferServices for nameservices: " +
                        Joiner.on(",").useForNull("<default>").join(toAdd));

                // 针对待添加集合toAdd中的每个nameserviceId，做以下处理
                for (String nsToAdd : toAdd) {

                    Map<String,InetSocketAddress> nnIdToAddr = addrMap.get(nsToAdd);
                    Map<String,InetSocketAddress> nnIdToLifelineAddr = lifelineAddrMap.get(nsToAdd);

                    ArrayList<InetSocketAddress> addrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                    ArrayList<InetSocketAddress> lifelineAddrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());

                    for (String nnId : nnIdToAddr.keySet()) {
                        // 从addrMap中根据nameserviceId获取对应Socket地址InetSocketAddress，创建集合addrs
                        addrs.add(nnIdToAddr.get(nnId));

                        lifelineAddrs.add(nnIdToLifelineAddr != null ?
                                nnIdToLifelineAddr.get(nnId) : null);
                    }
                    BPOfferService bpos = createBPOS(nsToAdd, addrs, lifelineAddrs);
                    // 将nameserviceId->BPOfferService的对应关系添加到集合bpByNameserviceId中
                    bpByNameserviceId.put(nsToAdd, bpos);
                    // 将BPOfferService添加到集合offerServices中
                    offerServices.add(bpos);
                }
            }
            // 启动所有BPOfferService，实际上是通过调用它的start()方法启动
            startAll();
        }

        // Step 4. Shut down old nameservices. This happens outside
        // of the synchronized(this) lock since they need to call
        // back to .remove() from another thread

        // 第4步，停止所有旧的nameservices。这个是发生在synchronized代码块外面的，是因为它们需要回调另外一个线程的remove()方法
        if (!toRemove.isEmpty()) {
            LOG.info("Stopping BPOfferServices for nameservices: " + Joiner.on(",").useForNull("<default>").join(toRemove));

            // 遍历待删除集合toRemove中的每个nameserviceId
            for (String nsToRemove : toRemove) {
                // 根据nameserviceId从集合bpByNameserviceId中获取BPOfferService
                BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
                // 调用BPOfferService的stop()和join()方法停止服务
                bpos.stop();
                bpos.join();
                // they will call remove on their own
                // 它们会调用本身的remove()方法
            }
        }

        // Step 5. Update nameservices whose NN list has changed
        // 第5步，更新NN列表已变化的nameservices
        if (!toRefresh.isEmpty()) {
            LOG.info("Refreshing list of NNs for nameservices: " +
                    Joiner.on(",").useForNull("<default>").join(toRefresh));

            // 遍历待更新集合toRefresh中的每个nameserviceId
            for (String nsToRefresh : toRefresh) {

                // 根据nameserviceId从集合bpByNameserviceId中取出对应的BPOfferService
                final BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);

                Map<String,InetSocketAddress> nnIdToAddr = addrMap.get(nsToRefresh);
                Map<String,InetSocketAddress> nnIdToLifelineAddr = lifelineAddrMap.get(nsToRefresh);
                final ArrayList<InetSocketAddress> addrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                final ArrayList<InetSocketAddress> lifelineAddrs = Lists.newArrayListWithCapacity(nnIdToAddr.size());
                for (String nnId : nnIdToAddr.keySet()) {
                    // 根据BPOfferService从配置信息addrMap中取出NN的Socket地址InetSocketAddress，形成列表addrs
                    addrs.add(nnIdToAddr.get(nnId));
                    lifelineAddrs.add(nnIdToLifelineAddr != null ? nnIdToLifelineAddr.get(nnId) : null);
                }
                try {
                    // 调用BPOfferService的refreshNNList()方法根据addrs刷新NN列
                    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Object>() {
                                            @Override
                                            public Object run() throws Exception {
                                                bpos.refreshNNList(addrs, lifelineAddrs);
                                                return null;
                                            }
                                        });
                } catch (InterruptedException ex) {
                    IOException ioe = new IOException();
                    ioe.initCause(ex.getCause());
                    throw ioe;
                }
            }
        }
    }

    /**
     * Extracted out for test purposes.
     */
    protected BPOfferService createBPOS(
            final String nameserviceId,
            List<InetSocketAddress> nnAddrs,
            List<InetSocketAddress> lifelineNnAddrs) {
        return new BPOfferService(nameserviceId, nnAddrs, lifelineNnAddrs, dn);
    }
}
