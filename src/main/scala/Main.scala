import deploy.HadoopEcoSystemDeployer
import deploy.util.SSHNode

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {
  val pwd = "zjuvlis"
  val cluster = Seq(
    SSHNode("10.214.208.11", "node1", "vlis", pwd),
    SSHNode("10.214.208.12", "node2", "vlis", pwd),
    SSHNode("10.214.208.13", "node3", "vlis", pwd),
    SSHNode("10.214.208.14", "node4", "vlis", pwd)
  )

  val nameNode = cluster.head
  val secondaryNameNode = cluster.take(2).last
  val HMaster = nameNode
  val sparkMaster = nameNode
  val HDFSDataDir = "dfs"
  val zooKeeperDataDir = "zookeeper"
  val sparkDataDir = "spark"

  HadoopEcoSystemDeployer.deploy(cluster, nameNode, secondaryNameNode, HMaster, sparkMaster, HDFSDataDir, zooKeeperDataDir, sparkDataDir)
}
