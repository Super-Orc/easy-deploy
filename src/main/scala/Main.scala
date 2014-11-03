import java.nio.file.Paths

import deploy.{RootNode, HadoopEcoSystemDeployer}

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {
  val pwd = "zjuvlis"
  val cluster = Seq(
    RootNode("10.214.208.11", "master", pwd),
    RootNode("10.214.208.12", "slave1", pwd),
    RootNode("10.214.208.13", "slave2", pwd),
    RootNode("10.214.208.14", "slave3", pwd)
  )

  val overwriteHostsFile = false
  val nameNode = cluster.head
  val secondaryNameNode = cluster.take(2).last
  val HMaster = secondaryNameNode
  val sparkMaster = cluster.take(3).last
  val HDFSDataDir = Paths.get("/home/hadoop/dfs")
  val zooKeeperDataDir = Paths.get("/home/hadoop/zookeeper")

  HadoopEcoSystemDeployer.deploy(cluster, overwriteHostsFile, nameNode, secondaryNameNode, HMaster, sparkMaster, HDFSDataDir, zooKeeperDataDir)
}
