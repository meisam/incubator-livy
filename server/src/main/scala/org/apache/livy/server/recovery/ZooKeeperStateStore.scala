/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.livy.server.recovery

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.UnhandledErrorListener
import org.apache.curator.framework.api.transaction.{CuratorTransaction => Transaction, CuratorTransactionFinal}
import org.apache.curator.framework.recipes.atomic.{DistributedAtomicLong => DistributedLong}
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.KeeperException.NoNodeException

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.LivyConf.Entry

object ZooKeeperStateStore {
  val ZK_KEY_PREFIX_CONF = Entry("livy.server.recovery.zk-state-store.key-prefix", "livy")
  val ZK_RETRY_CONF = Entry("livy.server.recovery.zk-state-store.retry-policy", "5,100")
}

class ZooKeeperStateStore(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None) // For testing
  extends StateStore(livyConf) with Logging {

  import org.apache.livy.server.recovery.ZooKeeperStateStore._

  // Constructor defined for StateStore factory to new this class using reflection.
  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val zkAddress = livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL)
  require(!zkAddress.isEmpty, s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL.key}.")
  private val zkKeyPrefix = livyConf.get(ZK_KEY_PREFIX_CONF)
  private val retryValue = livyConf.get(ZK_RETRY_CONF)
  // a regex to match patterns like "m, n" where m and n both are integer values
  private val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
  private[recovery] val retryPolicy = retryValue match {
    case retryPattern(n, sleepMs) => new RetryNTimes(n.toInt, sleepMs.toInt)
    case _ => throw new IllegalArgumentException(
      s"$ZK_KEY_PREFIX_CONF contains bad value: $retryValue. " +
        "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
  }

  private val curatorClient = mockCuratorClient.getOrElse {
    CuratorFrameworkFactory.newClient(zkAddress, retryPolicy)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      curatorClient.close()
    }
  }))

  curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener {
    def unhandledError(message: String, e: Throwable): Unit = {
      error(s"Fatal Zookeeper error. Shutting down Livy server.")
      System.exit(1)
    }
  })
  curatorClient.start()
  // TODO Make sure ZK path has proper secure permissions so that other users cannot read its
  // contents.

  override def set(key: String, value: Object): Unit = {
    val prefixedKey = prefixKey(key)
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(prefixedKey, data)
    } else {
      curatorClient.setData().forPath(prefixedKey, data)
    }
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      None
    } else {
      Option(deserialize[T](curatorClient.getData().forPath(prefixedKey)))
    }
  }

  override def getChildren(key: String): Seq[String] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      Seq.empty[String]
    } else {
      curatorClient.getChildren.forPath(prefixedKey).asScala
    }
  }

  override def remove(key: String): Unit = {
    try {
      curatorClient.delete().guaranteed().forPath(prefixKey(key))
    } catch {
      case _: NoNodeException =>
    }
  }

  override def nextValue(key: String): Long = {
    val distributedSessionId = new DistributedLong(curatorClient, key, retryPolicy)
    distributedSessionId.increment() match {
      case atomicValue if atomicValue.succeeded() =>
        atomicValue.postValue()
      case _ =>
        throw new java.io.IOException(s"Failed to atomically nextValue the next value for $key.")
    }
  }

  /**
   * @note
   *       To be used only once at startup and only when the layout of the recovery data changes.
   * Upgrades the recovery data layout from v1 to v2 in one transaction.
   * Either the operation succeeds or nothing happens.
   *
   * If the upgrade succeeds, the following is guaranteed:
   * All v1 data is copied to v2 data.
   * 1. The valued of `nextSessionId` is copied from
   *   `/path/do/livy/nodes/v1/interactive/state` to
   *   `/path/do/livy/nodes/v2/v9/interactive/nextSessionId` as a
   *   [[org.apache.curator.framework.recipes.atomic.DistributedAtomicLong]].
   * 1. The valued of `nextSessionId` is copied from
   *   `/path/do/livy/nodes/v1/batch/state` to
   *   `/path/do/livy/nodes/v2/v9/batch/nextSessionId` as a
   * *   [[org.apache.curator.framework.recipes.atomic.DistributedAtomicLong]].
   *
   * Only upgrade from v1 to v2 is supported in this implementation.
   * If keepOldVersion is set to false, the upgrade process deletes the old version data from ZK.
   * @param oldVersion
   * @param newVersion Must be set to v2 for this version of
   * @param keepOldVersion
   * @since 0.7
   */
  def upgrade(oldVersion: String, newVersion: String, keepOldVersion: Boolean = false): Unit = {
    val nextSessionIdPattern = """\{"nextSessionId":([0-9]+)\}""".r
    def isStateNode(zkNodePath: String) = {
      zkNodePath.endsWith("interactive/state") || zkNodePath.endsWith("batch/state")
    }

    @scala.annotation.tailrec
    def copyChildrenData(zkNodesToCopy: List[String], transaction: Transaction): Transaction = {
      zkNodesToCopy match {
        case Nil =>
          transaction
        case zkNode :: remainingZkNodeNodes =>
          val zkNodeData = if (isStateNode(zkNode)) {
            val jsonStateData = new String(curatorClient.getData.forPath(zkNode))
            val nextSessionIdPattern(lastUsedId) = jsonStateData
            val byteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)
            byteBuffer.putLong(lastUsedId.toLong).array()
          } else {
            curatorClient.getData.forPath(zkNode)
          }
          val zkNodeAcl = curatorClient.getACL.forPath(zkNode)

          val upgradedNodePath = if (isStateNode(zkNode)) {
            newVersion + zkNode.drop(oldVersion.length).dropRight("state".length) + "nextSessionId"
          } else {
            newVersion + zkNode.drop(oldVersion.length)
          }
          val nextTnx = transaction.create().withACL(zkNodeAcl)
            .forPath(upgradedNodePath, zkNodeData).and()

          val children = curatorClient.getChildren().forPath(zkNode).asScala.map {
            case child if zkNode == "/" => s"/$child"
            case child => s"$zkNode/$child"
          }
          copyChildrenData(remainingZkNodeNodes ++ children, nextTnx)
      }
    }

    def deleteOldVersion(zkNodesToDelete: List[String], transaction: Transaction): Transaction = {
      zkNodesToDelete match {
        case Nil =>
          transaction
        case zkNode :: remainingNodes =>

          val children = curatorClient.getChildren().forPath(zkNode).asScala.map {
            case child if zkNode == "/" => s"/$child"
            case child => s"$zkNode/$child"
          }
          val nextTransaction = deleteOldVersion(remainingNodes ++ children, transaction)
          nextTransaction.delete().forPath(zkNode).and()
      }
    }

    def validUpgrade: Boolean = {
      oldVersion != null &&
        newVersion != null &&
        newVersion.startsWith("/") &&
        oldVersion.startsWith("/") &&
        oldVersion.endsWith("/v1") &&
        newVersion.endsWith("/v2") &&
        !oldVersion.startsWith(newVersion ) &&
        !newVersion.startsWith(oldVersion)
    }

    def commit(transaction: Transaction): Unit = {
      transaction match {
        case finalTransaction: CuratorTransactionFinal => finalTransaction.commit()
        case _ => throw new RuntimeException(s"Failed to upgrade $oldVersion to $newVersion.")
      }
    }

    if (validUpgrade) {
      val runningTransaction = curatorClient.inTransaction().check().forPath(oldVersion).and()

      val copyTransaction = copyChildrenData(List(oldVersion), runningTransaction)

      val finalTransaction = if (keepOldVersion) {
        copyTransaction
      } else {
        deleteOldVersion(List(oldVersion), copyTransaction)
      }
      commit(finalTransaction)
    }
  }

  private def prefixKey(key: String) = s"/$zkKeyPrefix/$key"
}
