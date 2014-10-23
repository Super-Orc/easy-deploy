import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

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
  //installJDK(Seq(SSHNode("slave1", username, password)))
  println(getSoftwareFileName("jdk"))

  def addHosts(cluster: Seq[SSHNode]): Unit = {
    val hostsFileName = "hosts"
    for (node <- cluster) {
      node.sendFile(Paths.get(hostsFileName))
      node.ssh { sh =>
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
      node.ssh { sh =>
        import sh._
        cd(".ssh")
        rm(ls())
        execute("""ssh-keygen -t rsa -N "" -f id_rsa""")
        execute("echo 'StrictHostKeyChecking no\nUserKnownHostsFile /dev/null' > config")
        keys.write(cat("id_rsa.pub"))
        keys.newLine()
      }
    }
    keys.flush()
    cluster.foreach(_.sendFile(Paths.get(keysFileName), s".ssh/$keysFileName"))
    Files.delete(keysFile)
  }

  def installJDK(cluster: Seq[SSHNode]): Unit = {
    val jdkFileName = getSoftwareFileName("jdk")
    for (node <- cluster) {
      node.sendFile(Paths.get(s"software/$jdkFileName"), jdkFileName)
      node.ssh { sh =>
        import sh._
        execute(s"tar xzf $jdkFileName -C /opt")
        val jdkDirName = ls("/opt").find(_.contains("jdk")).get
        execute(s"ln -s /opt/$jdkDirName /usr/local/jdk")
        execute("echo 'export JAVA_HOME=/usr/local/jdk\nPATH=$JAVA_HOME/bin:$PATH' >> /etc/profile")
        rm(jdkFileName)
      }
    }
  }

  def getSoftwareFileName(keyword: String) = {
    import scala.collection.JavaConverters._
    val stream = Files.newDirectoryStream(Paths.get("software"))
    stream.iterator.asScala
      .filter(Files.isRegularFile(_))
      .map(_.getFileName.toString)
      .find(_.contains(keyword)).get
  }
}
