import java.nio.file.Paths

import deploy.HadoopEcoSystemDeployer
import deploy.util.SSHNode

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {
  val cluster = Seq(
    SSHNode("10.214.20.118", "node1", "vlis", "vlis@zju")
  )

  val nameNode = cluster.head
  val secondaryNameNode = cluster.take(2).last
  val HMaster = nameNode
  val sparkMaster = nameNode
  val HDFSDataDir = Paths.get("/home/hadoop/dfs")
  val zooKeeperDataDir = Paths.get("/home/hadoop/zookeeper")
  val sparkDataDir = Paths.get("/home/hadoop/spark")

  HadoopEcoSystemDeployer.deploy(cluster, nameNode, secondaryNameNode, HMaster, sparkMaster, HDFSDataDir, zooKeeperDataDir, sparkDataDir)
}
