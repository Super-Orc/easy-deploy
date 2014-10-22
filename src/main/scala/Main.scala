import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {


  val username = "root"
  val password = "zjuvlis"
  val cluster = Seq(
    Node("master", username, password),
    Node("slave1", username, password),
    Node("slave2", username, password),
    Node("slave3", username, password)
  )

  noPasswordSSH(cluster)

  def noPasswordSSH(cluster: Seq[Node]): Unit = {
    val keysFileName = "authorized_keys"
    val keysFile = Paths.get(keysFileName)
    val keys = Files.newBufferedWriter(keysFile, Charset.forName("utf-8"))
    for (node <- cluster) {
      jassh.SSH.shell(node.host, node.username, node.password) { sh =>
        import sh._
        cd(".ssh")
        rm(ls())
        execute("""ssh-keygen -t rsa -N "" -f id_rsa""")
        execute("echo \"StrictHostKeyChecking no\nUserKnownHostsFile /dev/null\" > config")
        keys.write(cat("id_rsa.pub"))
        keys.newLine()
      }
    }
    keys.flush()
    for (node <- cluster) {
      jassh.SSH.once(node.host, node.username, node.password)(_.send(keysFileName, s".ssh/$keysFileName"))
    }
    Files.delete(keysFile)
  }
}

case class Node(host: String, username: String, password: String)