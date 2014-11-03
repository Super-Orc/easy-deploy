package deploy

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-31.
 */
object HadoopEcoSystemDeployer {

  def deploy(
      cluster: Seq[SSHNode],
      overwriteHostsFile: Boolean,
      nameNode: SSHNode,
      secondaryNameNode: SSHNode,
      HMaster: SSHNode,
      sparkMaster: SSHNode,
      HDFSDataDir: Path,
      zooKeeperDataDir: Path): Unit = {
    addHosts(cluster, overwriteHostsFile)
    noPasswordSSH(cluster)
    installJDK(cluster)
    installHadoop(cluster, nameNode, secondaryNameNode, HDFSDataDir)
    installZooKeeper(cluster, zooKeeperDataDir)
    installHBase(cluster, HMaster, nameNode.host, zooKeeperDataDir, cluster.map(_.host))
    installKafka(cluster, cluster.map(_.host))
    installSpark(cluster, sparkMaster)
  }

  def addHosts(cluster: Seq[SSHNode], overwrite: Boolean): Unit = {
    val hosts = cluster.map(node => s"${node.ip} ${node.host}").mkString("\n", "\n", "\n")
    val hostsFile = generateTempFile("hosts", if (overwrite) "127.0.0.1 localhost" + hosts else hosts)
    modifyRemoteFile(cluster, "/etc", !overwrite, hostsFile)
  }

  def noPasswordSSH(cluster: Seq[SSHNode]): Unit = {
    val keysFileName = "authorized_keys"
    val keysFile = Paths.get(keysFileName)
    val keys = Files.newBufferedWriter(keysFile, Charset.forName("utf-8"))
    for (node <- cluster) {
      node.ssh { sh =>
        import sh._
        cd(".ssh")
        execute("rm -rf ./*")
        execute("""ssh-keygen -t rsa -N "" -f id_rsa""")
        execute("echo 'StrictHostKeyChecking no' > config")
        keys.write(cat("id_rsa.pub"))
        keys.newLine()
      }
    }
    keys.close()
    modifyRemoteFile(cluster, ".ssh", false, keysFile)
  }

  def installJDK(cluster: Seq[SSHNode]): Unit = {
    unpackSoftware(cluster, "jdk")((sh, _) => {
      sh.execute("echo 'export JAVA_HOME=/usr/local/jdk\nPATH=$JAVA_HOME/bin:$PATH' >> /etc/profile")
    }, sh => ())
  }

  private val xmlHeader =
    """|<?xml version="1.0" encoding="UTF-8"?>
       |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    """.stripMargin

  def installHadoop(cluster: Seq[SSHNode], master: SSHNode, secondMaster: SSHNode, dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val slaves = cluster.map(_.host).mkString("\n")
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

    val configDir = "/usr/local/hadoop/etc/hadoop"
    unpackSoftware(cluster, "hadoop")(prepareDataDir(dataDirName), sh => {
      import sh._
      cd(configDir)
      execute("""sed -i "25s#\${JAVA_HOME}#$JAVA_HOME#" hadoop-env.sh""")
    })

    val slavesFile = generateTempFile("slaves", slaves)
    val coreSiteFile = generateTempFile("core-site.xml", xmlHeader + coreSite)
    val hdfsSiteFile = generateTempFile("hdfs-site.xml", xmlHeader + hdfsSite)
    modifyRemoteFile(cluster, configDir, false, slavesFile, coreSiteFile, hdfsSiteFile)

    master.ssh { sh =>
      import sh._
      cd("/usr/local/hadoop")
      execute("bin/hdfs namenode -format")
      execute("sbin/start-dfs.sh")
    }
  }

  def installZooKeeper(cluster: Seq[SSHNode], dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val serverNames = cluster.map(_.host)
    val configDir = "/usr/local/zookeeper/conf"
    unpackSoftware(cluster, "zookeeper")(prepareDataDir(dataDirName), sh => {
      import sh._
      cd(configDir)
      execute("mv zoo_sample.cfg zoo.cfg")
      execute(s"sed -i '12s#/tmp/zookeeper#$dataDirName#' zoo.cfg")
      execute(s"echo '${serverNames.indexOf(sh.hostname) + 1}' > $dataDir/myid")
    })
    val serversConfig = serverNames.zipWithIndex.map(e => s"server.${e._2 + 1}=${e._1}:2888:3888").mkString("\n")
    val serversConfigFile = generateTempFile("zoo.cfg", serversConfig)
    modifyRemoteFile(cluster, configDir, true, serversConfigFile)
    for (node <- cluster) {
      node.ssh { sh =>
        sh.cd("/usr/local/zookeeper")
        sh.execute("bin/zkServer.sh start")
      }
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
    val configDir = "/usr/local/hbase/conf"
    unpackSoftware(cluster, "hbase")((_, _) => (), sh => {
      import sh._
      cd(configDir)
      execute("echo 'export HBASE_MANAGES_ZK=false' >> hbase-env.sh")
      execute("""echo "export JAVA_HOME=$JAVA_HOME" >> hbase-env.sh""")
    })
    val regionServersFile = generateTempFile("regionservers", cluster.map(_.host).mkString("\n"))
    val hbaseSiteFile = generateTempFile("hbase-site.xml", xmlHeader + hbaseSite)
    modifyRemoteFile(cluster, configDir, false, regionServersFile, hbaseSiteFile)
    master.ssh { sh =>
      sh.cd("/usr/local/hbase")
      sh.execute("bin/start-hbase.sh")
    }
  }

  def installKafka(cluster: Seq[SSHNode], zooKeeperCluster: Seq[String]): Unit = {
    val serverNames = cluster.map(_.host)
    val configDir = "/usr/local/kafka/config"
    unpackSoftware(cluster, "kafka")((_, _) => (), sh => {
      import sh._
      cd(configDir)
      execute(s"sed -i '20s#.*#broker.id=${serverNames.indexOf(sh.hostname)}#' server.properties")
      execute(s"sed -i '114d' server.properties")
    })
    val zkConfig = "zookeeper.connect=" + zooKeeperCluster.map(_ + ":2181").mkString(",")
    val zkConfigFile = generateTempFile("server.properties", zkConfig)
    modifyRemoteFile(cluster, configDir, true, zkConfigFile)

    for (node <- cluster) {
      node.ssh { sh =>
        sh.cd("/usr/local/kafka")
        sh.execute("bin/kafka-server-start.sh -daemon config/server.properties")
      }
    }
  }

  def installSpark(cluster: Seq[SSHNode], master: SSHNode): Unit = {
    val slaves = cluster.map(_.host).mkString("\n")
    unpackSoftware(cluster, "spark")((_, _) => (), _ => ())
    val slavesFile = generateTempFile("slaves", slaves)
    modifyRemoteFile(cluster, "/usr/local/spark/conf", false, slavesFile)
    master.ssh(_.execute("/usr/local/spark/sbin/start-all.sh"))
  }

  private def prepareDataDir(dataDirName: String)(sh: SSHShell, username: String): Unit = {
    import sh._
    if (exists(dataDirName)) {
      execute(s"rm -rf $dataDirName/*")
    } else {
      mkdir(dataDirName)
    }
    execute(s"chown $username.$username $dataDirName")
    execute(s"chmod 775 $dataDirName")
  }

  private def generateTempFile(name: String, content: String): Path = {
    val path = Paths.get(name)
    Files.write(path, content.getBytes("utf8"))
    path
  }

  private def modifyRemoteFile(
      cluster: Seq[SSHNode],
      remoteDir: String,
      isAppend: Boolean,
      localFiles: Path*): Unit = {
    for (node <- cluster) {
      for (localFile <- localFiles) {
        val localFileName = localFile.getFileName.toString
        val remoteDestination = s"$remoteDir/$localFileName"
        if (isAppend) {
          node.sendFile(localFile)
          node.sshWithRootShell { (sh, _) =>
            sh.execute(s"cat $localFileName >> $remoteDestination")
            sh.rm(localFileName)
          }
        } else {
          node.sendFile(localFile, remoteDestination)
        }
      }
    }
    localFiles.foreach(Files.delete)
  }

  private def unpackSoftware(cluster: Seq[SSHNode], keyword: String)
                            (withRootShell: (SSHShell, String) => Unit, withNormalShell: SSHShell => Unit): Unit = {
    def getSoftwareFileName(keyword: String) = {
      import scala.collection.JavaConverters._
      val stream = Files.newDirectoryStream(Paths.get("software"))
      stream.iterator.asScala
        .filter(Files.isRegularFile(_))
        .map(_.getFileName.toString)
        .filter(_.contains(keyword))
        .minBy(_.size)
    }

    val softwareFileName = getSoftwareFileName(keyword)
    for (node <- cluster) {
      println(s"install $keyword to ${node.host} ...")
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
      println(s"install $keyword to ${node.host} done")
    }
  }
}
