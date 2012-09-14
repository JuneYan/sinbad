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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy.NotEnoughReplicasException;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.HostsFileReader;

import java.util.*;

/**
 * The class is responsible for choosing the desired number of targets for
 * placing block replicas with network conditions in mind at every step. The
 * replica placement strategy is that if the writer is on a datanode, the 1st
 * replica is placed on the local machine, otherwise pick a datanode with
 * network awareness. The 2nd replica is placed on a datanode that is on a
 * different rack. The 3rd replica is placed on a datanode which is on a
 * different node of the rack as the second replica.
 */
public class BlockPlacementPolicyNetAware extends BlockPlacementPolicy {
  protected NetworkTopology clusterMap;
  private int attemptMultiplier = 0;

  BlockPlacementPolicyNetAware(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap) {

    initialize(conf, stats, clusterMap, null, null, null);
  }

  BlockPlacementPolicyNetAware() {
  }

  /** {@inheritDoc} */
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, HostsFileReader hostsReader,
      DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem ns) {
    // First, set oldFactor in the superclass BlockPlacementPolicy
    super.oldFactor = conf.getFloat("dfs.replication.netaware.oldfactor", (float) 0.2);
    // Then, do everything else
    this.clusterMap = clusterMap;
    Configuration newConf = new Configuration();
    this.attemptMultiplier = newConf.getInt("dfs.replication.attemptMultiplier", 200);
  }

  @Override
  public void hostsUpdated() {
    // Do nothing in this case
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null, blocksize);
  }

  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor[] chooseTarget(String srcInode,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    List<Node> excludesNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, excludesNodes, blocksize);
  }
  
  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor[] chooseTarget(FSInodeInfo srcInode,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    List<Node> excludesNodes,
                                    long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null, blocksize);
  }

  /**
   * This is not part of the public API but is used by the unit tests.
   */
  synchronized DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    List<Node> exlcNodes,
                                    long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return new DatanodeDescriptor[0];
    }
      
    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    if (exlcNodes != null) {
      for (Node node:exlcNodes) {
        excludedNodes.put(node, node);
      }
    }
     
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = chosenNodes.size()+numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
      
    int maxNodesPerRack = 
      (totalNumOfReplicas-1)/clusterMap.getNumOfRacks()+2;
      
    List<DatanodeDescriptor> results = 
      new ArrayList<DatanodeDescriptor>(chosenNodes);
    for (Node node:chosenNodes) {
      excludedNodes.put(node, node);
    }
      
    if (!clusterMap.contains(writer)) {
      writer=null;
    }
      
    DatanodeDescriptor localNode = chooseTarget(numOfReplicas, writer, 
                                                excludedNodes, blocksize, maxNodesPerRack, results);
      
    results.removeAll(chosenNodes);
      
    // sorting nodes to form a pipeline
    DatanodeDescriptor[] selectedOnes = getPipeline((writer==null)?localNode:writer,
                       results.toArray(new DatanodeDescriptor[results.size()]));
    
    // Update network usage of the selected ones 
    for (DatanodeDescriptor dd: selectedOnes) {
      // Bump up the RxBps based on blocksize
      LOG.info("chooseTarget selected " + dd.getName()
          + " with RxBps = " + (dnNameToRxBpsMap.containsKey(dd.getName()) ? dnNameToRxBpsMap.get(dd.getName()) : 0.0));
      adjustRxBps(dd.getName(), blocksize);
    }
    
    return selectedOnes;
  }
    
  /* choose <i>numOfReplicas</i> from all data nodes */
  protected DatanodeDescriptor chooseTarget(int numOfReplicas,
                                          DatanodeDescriptor writer,
                                          HashMap<Node, Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results) {
      
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return writer;
    }
      
    int numOfResults = results.size();
    boolean newBlock = (numOfResults==0);
    if (writer == null && !newBlock) {
      writer = results.get(0);
    }
      
    try {
      if (numOfResults == 0) {
        writer = chooseLocalNode(writer, excludedNodes, 
                                 blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 1) {
        chooseRemoteRack(1, results.get(0), excludedNodes, 
                         blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        if (clusterMap.isOnSameRack(results.get(0), results.get(1))) {
          chooseRemoteRack(1, results.get(0), excludedNodes,
                           blocksize, maxNodesPerRack, results);
        } else if (newBlock){
          chooseLocalRack(results.get(1), excludedNodes, blocksize, 
                          maxNodesPerRack, results);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize,
                          maxNodesPerRack, results);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseNetworkAware(numOfReplicas, NodeBase.ROOT, excludedNodes, 
                   blocksize, maxNodesPerRack, results, writer);
    } catch (NotEnoughReplicasException e) {
      FSNamesystem.LOG.warn("Not able to place enough replicas, still in need of "
               + numOfReplicas);
    }
    return writer;
  }

  /* choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack using network awareness
   * @return the chosen node
   */
  protected DatanodeDescriptor chooseLocalNode(
                                             DatanodeDescriptor localMachine,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // if no local machine, choose a node using network awareness
    if (localMachine == null)
      return chooseNetworkAware(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results, localMachine);
      
    // otherwise try local machine first
    Node oldNode = excludedNodes.put(localMachine, localMachine);
    if (oldNode == null) { // was not in the excluded list
      results.add(localMachine);
      return localMachine;
    } 
      
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, 
                           blocksize, maxNodesPerRack, results);
  }
    
  /* choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a node using network awareness 
   * in the cluster.
   * @return the chosen node
   */
  protected DatanodeDescriptor chooseLocalRack(
                                             DatanodeDescriptor localMachine,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // no local machine, so choose a node using network awareness
    if (localMachine == null) {
      return chooseNetworkAware(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results, localMachine);
    }
      
    // choose one from the local rack
    try {
      return chooseNetworkAware(
                          localMachine.getNetworkLocation(),
                          excludedNodes, blocksize, maxNodesPerRack, results, localMachine);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(Iterator<DatanodeDescriptor> iter=results.iterator();
          iter.hasNext();) {
        DatanodeDescriptor nextNode = iter.next();
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseNetworkAware(
                              newLocal.getNetworkLocation(),
                              excludedNodes, blocksize, maxNodesPerRack, results, localMachine);
        } catch(NotEnoughReplicasException e2) {
          //otherwise choose a node using network awareness
          return chooseNetworkAware(NodeBase.ROOT, excludedNodes,
                              blocksize, maxNodesPerRack, results, localMachine);
        }
      } else {
        //otherwise choose a node using network awareness
        return chooseNetworkAware(NodeBase.ROOT, excludedNodes,
                            blocksize, maxNodesPerRack, results, localMachine);
      }
    }
  }
    
  /* choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones 
   * from the local rack
   */
    
  protected void chooseRemoteRack(int numOfReplicas,
                                DatanodeDescriptor localMachine,
                                HashMap<Node, Node> excludedNodes,
                                long blocksize,
                                int maxReplicasPerRack,
                                List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // choose a node using network awareness
    try {
      chooseNetworkAware(numOfReplicas, "~"+localMachine.getNetworkLocation(),
                   excludedNodes, blocksize, maxReplicasPerRack, results, localMachine);
    } catch (NotEnoughReplicasException e) {
      chooseNetworkAware(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, localMachine);
    }
  }

  /* Choose one target from <i>nodes</i> with network-awareness.
   * @return the chosen node
   */
  private DatanodeDescriptor chooseNetworkAware(
                                          String nodes,
                                          HashMap<Node, Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results,
                                          DatanodeDescriptor localMachine) 
    throws NotEnoughReplicasException {
    int numOfAvailableNodes =
      clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    while(numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = 
        pickMinLoadedNode(clusterMap.getAvailableLeaves(nodes, excludedNodes.keySet()), localMachine);

      Node oldNode = excludedNodes.put(chosenNode, chosenNode);
      if (oldNode == null) { // chosenNode was not in the excluded list
        numOfAvailableNodes--;
        results.add(chosenNode);
        return chosenNode;
      }
    }

    throw new NotEnoughReplicasException(
        "Not able to place enough replicas");
  }
   
  /* Choose <i>numOfReplicas</i> targets from <i>nodes</i> with network awareness.
   */
  void chooseNetworkAware(int numOfReplicas,
                    String nodes,
                    HashMap<Node, Node> excludedNodes,
                    long blocksize,
                    int maxNodesPerRack,
                    List<DatanodeDescriptor> results,
                    DatanodeDescriptor localMachine)
    throws NotEnoughReplicasException {

    int numOfAvailableNodes =
      clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    int numAttempts = numOfAvailableNodes * this.attemptMultiplier;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0 && --numAttempts > 0) {
      DatanodeDescriptor chosenNode = 
        pickMinLoadedNode(clusterMap.getAvailableLeaves(nodes, excludedNodes.keySet()), localMachine);
      Node oldNode = excludedNodes.put(chosenNode, chosenNode);
      if (oldNode == null) {
        numOfAvailableNodes--;
        numOfReplicas--;
        results.add(chosenNode);
      }
    }
      
    if (numOfReplicas>0) {
      throw new NotEnoughReplicasException(
                                           "Not able to place enough replicas");
    }
  }

  /**
   * return the node that has the least RxBps
   * @param candidates 
   * @return
   */
  private DatanodeDescriptor pickMinLoadedNode(Collection<Node> candidates, DatanodeDescriptor localMachine) {
    double minRxBps = Double.MAX_VALUE;
    Node retVal = null;
    
    // Cap at source's txBps
    double maxRxBps = dnNameToTxBpsMap.get(localMachine.getName());
    
    for (Node cand: candidates) {
      Double candRxBps = dnNameToRxBpsMap.get(cand.getName());
      if (candRxBps == null) {
        candRxBps = Double.MAX_VALUE;
      }
      if (candRxBps <= minRxBps && (retVal == null || candRxBps >= maxRxBps)) {
        retVal = cand;
        minRxBps = candRxBps;
      }
//      LOG.info("pickMinLoadedNode examining " + cand.getName()
//          + " with RxBps = " + candRxBps);
    }
//    LOG.info("pickMinLoadedNode selected " + retVal.getName()
//        + " with RxBps = " + minRxBps);
    return (DatanodeDescriptor) retVal;
  }
  
  /* Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  protected DatanodeDescriptor[] getPipeline(
                                           DatanodeDescriptor writer,
                                           DatanodeDescriptor[] nodes) {
    if (nodes.length==0) return nodes;
      
    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = nodes[0];
      }
      for(;index<nodes.length; index++) {
        DatanodeDescriptor shortestNode = nodes[index];
        int shortestDistance = clusterMap.getDistance(writer, shortestNode);
        int shortestIndex = index;
        for(int i=index+1; i<nodes.length; i++) {
          DatanodeDescriptor currentNode = nodes[i];
          int currentDistance = clusterMap.getDistance(writer, currentNode);
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestNode = currentNode;
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          nodes[shortestIndex] = nodes[index];
          nodes[index] = shortestNode;
        }
        writer = shortestNode;
      }
    }
    return nodes;
  }

  /** {@inheritDoc} */
  public int verifyBlockPlacement(String srcPath,
                                  LocatedBlock lBlk,
                                  int minRacks) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = new DatanodeInfo[0];
    int numRacks = clusterMap.getNumOfRacks();
    if(numRacks <= 1) // only one rack
      return 0;
    minRacks = Math.min(minRacks, numRacks);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return minRacks - racks.size();
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
                                                 Block block,
                                                 short replicationFactor,
                                                 Collection<DatanodeDescriptor> first, 
                                                 Collection<DatanodeDescriptor> second) {
    long minSpace = Long.MAX_VALUE;
    DatanodeDescriptor cur = null;

    // pick replica from the first Set. If first is empty, then pick replicas
    // from second set.
    Iterator<DatanodeDescriptor> iter =
          first.isEmpty() ? second.iterator() : first.iterator();

    // pick node with least free space
    while (iter.hasNext() ) {
      DatanodeDescriptor node = iter.next();
      long free = node.getRemaining();
      if (minSpace > free) {
        minSpace = free;
        cur = node;
      }
    }
    return cur;
  }

}
