/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node; 
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.*;

/** 
 * This interface is used for choosing the desired number of targets
 * for placing block replicas.
 */
public abstract class BlockPlacementPolicy {
  
  // Keep track of the Configuration
  static Configuration confCopy = null;
  
  private class BpsInfo {
    double bps = 0.0;
    double tempBps = 0.0;
    boolean isTemp = false;
    long lastUpdateTime = now();
    
    void resetToNormal(double bps) {
      this.bps = bps;
      this.tempBps = bps;
      this.isTemp = false;
      this.lastUpdateTime = now();
    }
    
    void moveToTemp(double blockSize) {
      // Into the temporary zone
      this.isTemp = true;

      // 1Gbps == 128MBps
      double nicSpeed = 128.0 * 1024 * 1024;
      
      // Aim to increase by the remaining capacity of the link
      double incVal = nicSpeed - this.tempBps;
      if (incVal < 0.0) {
        incVal = 0.0;
      }
      
      // Calculate the expected time till the next update
      double secElapsed = (now() - lastUpdateTime) / 1000.0;
      double timeTillUpdate = 1.0 * confCopy.getInt("dfs.heartbeat.interval", 1) - secElapsed;

      // Bound incVal by blockSize
      if (timeTillUpdate > 0.0) {
        double temp = blockSize / timeTillUpdate;
        if (temp < incVal) {
          incVal = temp;
        }
      }
      
      this.tempBps = this.tempBps + incVal;
    }
    
    private long now() {
      return System.currentTimeMillis();
    }
  }
  
  public static final Log LOG = LogFactory.getLog(BlockPlacementPolicy.class.getName());
  
  // Keep track of receiving throughput at each DataNode
  private Map<String, BpsInfo> dnNameToRxBpsMap = Collections.synchronizedMap(new HashMap<String, BpsInfo>());
  // private Map<String, Double> dnNameToTxBpsMap = Collections.synchronizedMap(new HashMap<String, Double>());
  
  public double oldFactor = 0.0;

  public static class NotEnoughReplicasException extends Exception {
    private static final long serialVersionUID = 1L;
    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }
    
  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * 
   * @param srcPath the file to which this chooseTargets is being invoked. 
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target 
   * and sorted as a pipeline.
   */
  abstract DatanodeDescriptor[] chooseTarget(String srcPath,
                                             int numOfReplicas,
                                             DatanodeDescriptor writer,
                                             List<DatanodeDescriptor> chosenNodes,
                                             long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * The base implemenatation extracts the pathname of the file from the
   * specified srcInode, but this could be a costly operation depending on the
   * file system implementation. Concrete implementations of this class should
   * override this method to avoid this overhead.
   * 
   * @param srcPath the file to which this chooseTargets is being invoked.
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param excludesNodes datanodes to exclude as possible targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target 
   * and sorted as a pipeline.
   */
  abstract DatanodeDescriptor[] chooseTarget(String srcInode,
                                             int numOfReplicas,
                                             DatanodeDescriptor writer,
                                             List<DatanodeDescriptor> chosenNodes,
                                             List<Node> excludesNodes,
                                             long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i>
   * to re-replicate a block with size <i>blocksize</i>
   * If not, return as many as we can.
   * The base implemenatation extracts the pathname of the file from the
   * specified srcInode, but this could be a costly operation depending on the
   * file system implementation. Concrete implementations of this class should
   * override this method to avoid this overhead.
   *
   * @param srcInode The inode of the file for which chooseTarget is being invoked.
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target
   * and sorted as a pipeline.
   */
  DatanodeDescriptor[] chooseTarget(FSInodeInfo srcInode,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    List<Node> excludesNodes,
                                    long blocksize) {
    return chooseTarget(FSNamesystem.getFullPathName(srcInode), numOfReplicas, writer,
                        chosenNodes, blocksize);
  }

  /**
   * Verify that the block is replicated on at least minRacks different racks
   * if there is more than minRacks rack in the system.
   * 
   * @param srcPath the full pathname of the file to be verified
   * @param lBlk block with locations
   * @param minRacks number of racks the block should be replicated to
   * @return the difference between the required and the actual number of racks
   * the block is replicated to.
   */
  abstract public int verifyBlockPlacement(String srcPath,
                                           LocatedBlock lBlk,
                                           int minRacks);
  /**
   * Decide whether deleting the specified replica of the block still makes 
   * the block conform to the configured block placement policy.
   * 
   * @param srcInode The inode of the file to which the block-to-be-deleted belongs
   * @param block The block to be deleted
   * @param replicationFactor The required number of replicas for this block
   * @param existingReplicas The replica locations of this block that are present
                  on at least two unique racks. 
   * @param moreExistingReplicas Replica locations of this block that are not
                   listed in the previous parameter.
   * @return the replica that is the best candidate for deletion
   */
  abstract public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo srcInode,
                                      Block block, 
                                      short replicationFactor,
                                      Collection<DatanodeDescriptor> existingReplicas,
                                      Collection<DatanodeDescriptor> moreExistingReplicas);

  /**
   * Used to setup a BlockPlacementPolicy object. This should be defined by 
   * all implementations of a BlockPlacementPolicy.
   * 
   * @param conf the configuration object
   * @param stats retrieve cluster status from here
   * @param clusterMap cluster topology
   * @param namesystem the FSNamesystem
   */
  abstract protected void initialize(Configuration conf,  FSClusterStats stats, 
                                     NetworkTopology clusterMap,
                                     HostsFileReader hostsReader,
                                     DNSToSwitchMapping dnsToSwitchMapping,
                                     FSNamesystem namesystem);

  /**
   * Notifies class that include and exclude files have changed.
   */
  abstract public void hostsUpdated();

  /**
   * Get an instance of the configured Block Placement Policy based on the
   * value of the configuration paramater dfs.block.replicator.classname.
   * 
   * @param conf the configuration to be used
   * @param stats an object thatis used to retrieve the load on the cluster
   * @param clusterMap the network topology of the cluster
   * @param namesystem the FSNamesystem
   * @return an instance of BlockPlacementPolicy
   */
  public static BlockPlacementPolicy getInstance(Configuration conf, 
                                                 FSClusterStats stats,
                                                 NetworkTopology clusterMap,
                                                 HostsFileReader hostsReader,
                                                 DNSToSwitchMapping dnsToSwitchMapping,
                                                 FSNamesystem namesystem) {
    // Store the configuration
    BlockPlacementPolicy.confCopy = conf;
    
    Class<? extends BlockPlacementPolicy> replicatorClass =
                      conf.getClass("dfs.block.replicator.classname",
                                    BlockPlacementPolicyDefault.class,
                                    BlockPlacementPolicy.class);
    BlockPlacementPolicy replicator = (BlockPlacementPolicy) ReflectionUtils.newInstance(
                                                             replicatorClass, conf);
    replicator.initialize(conf, stats, clusterMap, hostsReader, 
                          dnsToSwitchMapping, namesystem);
    return replicator;
  }

  /**
   * choose <i>numOfReplicas</i> nodes for <i>writer</i> to replicate
   * a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * 
   * @param srcPath a string representation of the file for which chooseTarget is invoked
   * @param numOfReplicas number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as targets
   * and sorted as a pipeline.
   */
  DatanodeDescriptor[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    long blocksize) {
    return chooseTarget(srcPath, numOfReplicas, writer,
                        new ArrayList<DatanodeDescriptor>(),
                        blocksize);
  }
  
  /**
   * Update dnPathToRxBpsMap 
   * 
   * @param dnName path to DataNode
   * @param rxBps receiving throughout of the DataNode
   * @param txBps transmitting throughput of the DataNode
   */
  public void updateNetworkInformation(String dnName, double newRxBps, double newTxBps) {
    BpsInfo bpsInfo = (dnNameToRxBpsMap.containsKey(dnName)) ? dnNameToRxBpsMap.get(dnName) : new BpsInfo();
    double rxBps = (1.0 - this.oldFactor) * newRxBps + this.oldFactor * bpsInfo.bps;
    bpsInfo.resetToNormal(rxBps);
    dnNameToRxBpsMap.put(dnName, bpsInfo);

    // double oldTxBps = (dnNameToTxBpsMap.containsKey(dnName)) ? dnNameToTxBpsMap.get(dnName) : 0.0;
    // double txBps = (1.0 - this.oldFactor) * newTxBps + this.oldFactor * oldTxBps;
    // dnNameToTxBpsMap.put(dnName, txBps);

    // LOG.info(dnName + ": updatedRxBps = " + rxBps + " oldRxBps = " + oldRxBps + " oldFactor = " + this.oldFactor);
}
  
  public void adjustRxBps(String dnName, double blockSize) {
    BpsInfo bpsInfo = (dnNameToRxBpsMap.containsKey(dnName)) ? dnNameToRxBpsMap.get(dnName) : new BpsInfo();
    bpsInfo.moveToTemp(blockSize);
    dnNameToRxBpsMap.put(dnName, bpsInfo);
  }
  
  public double getDnRxBps(String dnName) {
    BpsInfo bpsInfo = dnNameToRxBpsMap.containsKey(dnName) ? dnNameToRxBpsMap.get(dnName) : new BpsInfo();
    return bpsInfo.isTemp ? bpsInfo.tempBps : bpsInfo.bps;
  }
}
