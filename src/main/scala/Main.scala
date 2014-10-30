import java.nio.charset.Charset
import java.nio.file.{Path, Files, Paths}

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-21.
 */
object Main extends App {
  val xmlHeader =
    """|<?xml version="1.0" encoding="UTF-8"?>
       |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    """.stripMargin

  val password = "zjuvlis"
  val cluster = Seq(
    RootNode("master", password),
    RootNode("slave1", password),
    RootNode("slave2", password),
    RootNode("slave3", password)
  )

  val test = NormalNode("fuck", "vlis", "vlis@zju", "vlis@zju")

  //addHosts(cluster)
  //noPasswordSSH(cluster)
  //addHosts(Seq(NormalNode("slave1", "cloud", "qwerty", password)))
  //addHosts(Seq(RootNode("slave1", password)))
  //installJDK(Seq(NormalNode("10.214.20.118", "vlis", "vlis@zju", "vlis@zju")))
  //installHadoop(Seq(NormalNode("fuck", "vlis", "vlis@zju", "vlis@zju")), Paths.get("/home/hadoop/dfs"))
  //installZooKeeper(Seq(NormalNode("fuck", "vlis", "vlis@zju", "vlis@zju")), Paths.get("/home/hadoop/zookeeper"))
  //installHBase(Seq(test), test, test.host, Paths.get("/home/hadoop/zookeeper"), Seq(test.host))

  def addHosts(cluster: Seq[SSHNode]): Unit = {
    val hostsFileName = "hosts"
    for (node <- cluster) {
      node.sendFile(Paths.get(hostsFileName))
      node.sshWithRootShell { (sh, _) =>
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
    unpackSoftware(cluster, "jdk")((sh, _) => {
      sh.execute("echo 'export JAVA_HOME=/usr/local/jdk\nPATH=$JAVA_HOME/bin:$PATH' >> /etc/profile")
    }, sh => ())
  }

  def installHadoop(cluster: Seq[SSHNode], master: SSHNode, secondMaster: SSHNode, dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val slaves = cluster.map(_.host).mkString("", "\n", "\n")
    val coreSite =
      s"""
        |<configuration>
        |  <property>
        |    <name>fs.defaultFS</name>
        |    <value>hdfs://${master.host}:9000</value>
        |  </property>
        |  <property>
        |    <name>io.file.buffer.size</name>
        |    <value>131072</value>
        |  </property>
        |</configuration>
      """.stripMargin
    val hdfsSite =
      s"""
        |<configuration>
        |  <property>
        |    <name>dfs.replication</name>
        |    <value>3</value>
        |  </property>
        |  <property>
        |    <name>dfs.namenode.secondary.http-address</name>
        |    <value>${secondMaster.host}:50090</value>
        |  </property>
        |  <property>
        |    <name>dfs.namenode.name.dir</name>
        |    <value>$dataDirName/name</value>
        |  </property>
        |  <property>
        |    <name>dfs.datanode.data.dir</name>
        |    <value>$dataDirName/data</value>
        |  </property>
        |</configuration>
      """.stripMargin
    unpackSoftware(cluster, "hadoop")(prepareDataDir(dataDirName), sh => {
      import sh._
      cd("/usr/local/hadoop/etc/hadoop")
      execute(s"echo '$slaves' > slaves")
      execute(s"echo '$xmlHeader' > core-site.xml")
      execute(s"echo '$coreSite' >> core-site.xml")
      execute(s"echo '$xmlHeader' > hdfs-site.xml")
      execute(s"echo '$hdfsSite' >> hdfs-site.xml")
      execute("""sed -i "25s#\${JAVA_HOME}#$JAVA_HOME#" hadoop-env.sh""")
    })
    master.ssh { sh =>
      import sh._
      cd("/usr/local/hadoop")
      execute("bin/hdfs namenode -format")
      execute("sbin/start-dfs.sh")
    }
  }

  def installZooKeeper(cluster: Seq[SSHNode], dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val servers = cluster.map(_.host).zipWithIndex.map(e => Tuple2(e._2 + 1, e._1))
    val serversConfig = servers.map(e => s"server.${e._1}=${e._2}:2888:3888").mkString("\n")
    unpackSoftware(cluster, "zookeeper")(prepareDataDir(dataDirName), sh => {
      import sh._
      cd("/usr/local/zookeeper/conf")
      execute("mv zoo_sample.cfg zoo.cfg")
      execute(s"sed -i '12s#/tmp/zookeeper#$dataDirName#' zoo.cfg")
      execute(s"echo '$serversConfig' >> zoo.cfg")
      execute(s"echo '${servers.find(_._2 == sh.hostname).map(_._1).get}' > $dataDir/myid")
    })
    cluster.last.ssh { sh =>
      sh.cd("/usr/local/zookeeper")
      sh.execute("bin/zkServer.sh start")
    }
  }

  def installHBase(
      cluster: Seq[SSHNode],
      master: SSHNode,
      nameNode: String,
      zooKeeperDataDir: Path,
      zooKeeperCluster: Seq[String]): Unit = {
    val hbaseSite =
      s"""
        |<configuration>
        |  <property>
        |    <name>hbase.cluster.distributed</name>
        |    <value>true</value>
        |  </property>
        |  <property>
        |    <name>hbase.rootdir</name>
        |    <value>hdfs://$nameNode:9000/hbase</value>
        |  </property>
        |  <property>
        |    <name>hbase.rootdir</name>
        |    <value>hdfs://$nameNode:9000/hbase</value>
        |  </property>
        |  <property>
        |    <name>hbase.zookeeper.quorum</name>
        |    <value>${zooKeeperCluster.mkString(",")}</value>
        |  </property>
        |  <property>
        |    <name>hbase.zookeeper.property.dataDir</name>
        |    <value>${zooKeeperDataDir.toAbsolutePath.toString}</value>
        |  </property>
        |</configuration>
      """.stripMargin
    unpackSoftware(cluster, "hbase")((_, _) => (), sh => {
      import sh._
      cd("/usr/local/hbase/conf")
      execute("echo 'export HBASE_MANAGES_ZK=false' >> hbase-env.sh")
      execute("""echo "export JAVA_HOME=$JAVA_HOME" >> hbase-env.sh""")
      execute(s"echo '${cluster.map(_.host).mkString("\n")}' > regionservers")
      execute(s"echo '$xmlHeader' > hbase-site.xml")
      execute(s"echo '$hbaseSite' >> hbase-site.xml")
    })
    master.ssh { sh =>
      sh.cd("/usr/local/hbase")
      sh.execute("bin/start-hbase.sh")
    }
  }

  def prepareDataDir(dataDirName: String)(sh: SSHShell, username: String): Unit = {
    import sh._
    if (exists(dataDirName)) {
      execute(s"rm -rf $dataDirName/*")
    } else {
      mkdir(dataDirName)
    }
    execute(s"chown $username.$username $dataDirName")
    execute(s"chmod 775 $dataDirName")
  }

  def unpackSoftware(cluster: Seq[SSHNode], keyword: String)
                    (withRootShell: (SSHShell, String) => Unit, withNormalShell: SSHShell => Unit): Unit = {
    def getSoftwareFileName(keyword: String) = {
      import scala.collection.JavaConverters._
      val stream = Files.newDirectoryStream(Paths.get("software"))
      stream.iterator.asScala
        .filter(Files.isRegularFile(_))
        .map(_.getFileName.toString)
        .find(_.contains(keyword)).get
    }

    val softwareFileName = getSoftwareFileName(keyword)
    for (node <- cluster) {
      node.sendFile(Paths.get(s"software/$softwareFileName"), softwareFileName)
      node.sshWithRootShell { (sh, username) =>
        import sh._
        ls("/tmp").filter(_.contains(keyword)).foreach(f => execute(s"rm -rf /tmp/$f"))
        execute(s"tar xzf $softwareFileName -C /tmp")
        val softwareDirName = ls("/tmp").find(_.contains(keyword)).get
        execute(s"rm -rf /opt/$softwareDirName")
        execute(s"cp -r /tmp/$softwareDirName /opt")
        execute(s"rm -rf /tmp/$softwareDirName")
        execute(s"chown -R $username.$username /opt/$softwareDirName")
        execute(s"rm -f /usr/local/$keyword")
        execute(s"ln -s /opt/$softwareDirName /usr/local/$keyword")
        rm(softwareFileName)
        withRootShell(sh, username)
      }
      node.ssh(withNormalShell)
    }
  }
}
