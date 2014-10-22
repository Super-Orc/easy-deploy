import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import fr.janalyse.ssh.{SSH, SSHShell}

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {


  val username = "root"
  val password = "zjuvlis"
  val cluster = Seq(
    SSHNode("master", username, password),
    SSHNode("slave1", username, password),
    SSHNode("slave2", username, password),
    SSHNode("slave3", username, password)
  )

  //addHosts(cluster)
  //noPasswordSSH(cluster)

  def addHosts(cluster: Seq[SSHNode]): Unit = {
    val hostsFileName = "hosts"
    for (node <- cluster) {
      node.once(_.send(hostsFileName))
      node.shell { sh =>
        sh.execute(s"cat $hostsFileName >> /etc/$hostsFileName")
        sh.rm(hostsFileName)
      }
    }
  }

  def noPasswordSSH(cluster: Seq[SSHNode]): Unit = {
    val keysFileName = "authorized_keys"
    val keysFile = Paths.get(keysFileName)
    val keys = Files.newBufferedWriter(keysFile, Charset.forName("utf-8"))
    for (node <- cluster) {
      node.shell { sh =>
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
      node.once(_.send(keysFileName, s".ssh/$keysFileName"))
    }
    Files.delete(keysFile)
  }
}

case class SSHNode(host: String, username: String, password: String) {

  def once(withssh: SSH => Unit): Unit = {
    jassh.SSH.once(host, username, password)(withssh)
  }
  
  def shell(withsh: SSHShell => Unit): Unit = {
    jassh.SSH.shell(host, username, password)(withsh)
  }
}