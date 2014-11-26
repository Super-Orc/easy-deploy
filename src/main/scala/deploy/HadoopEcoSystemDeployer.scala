package deploy

import java.nio.file.{Files, Path, Paths}

import deploy.util.ExtendFunction._
import deploy.util.SSHNode
import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-31.
 */
object HadoopEcoSystemDeployer {

  val deployDir = "/opt/Titans"

  def deploy(
      cluster: Seq[SSHNode],
      nameNode: SSHNode,
      secondaryNameNode: SSHNode,
      HMaster: SSHNode,
      sparkMaster: SSHNode,
      HDFSDataDir: Path,
      zooKeeperDataDir: Path,
      sparkDataDir: Path): Unit = {
    clean(cluster)
    addHosts(cluster)
    noPasswordSSH(cluster)
    disableFirewall(cluster)
    installJDK(cluster)
    installHadoop(cluster, nameNode, secondaryNameNode, HDFSDataDir)
    installZooKeeper(cluster, zooKeeperDataDir)
    installHBase(cluster, HMaster, nameNode.hostname, zooKeeperDataDir, cluster)
    installKafka(cluster, cluster)
    installSpark(cluster, sparkMaster, sparkDataDir)
  }

  def clean(cluster: Seq[SSHNode]): Unit = {
    cluster.foreach(_.ssh(prepareDataDir(deployDir, _)))
  }

  def addHosts(cluster: Seq[SSHNode]): Unit = {
    val hosts = cluster.map(node => s"${node.ip} ${node.hostname}").mkString("\n", "\n", "\n")
    modifyRemoteFile(cluster, "/etc", true, true, generateTempFile("hosts", hosts))
    for (node <- cluster) {
      node.ssh(_.sudo(s"hostname ${node.hostname}")) //TODO
    }
    generateTempFile("hosts", hosts)
  }

  def noPasswordSSH(cluster: Seq[SSHNode]): Unit = {
    cluster.head.ssh(_.execute("""ssh-keygen -t rsa -N "" -f id_rsa"""))
    val publicKeyFile = cluster.head.getFile("id_rsa.pub", "authorized_keys")
    val privateKeyFile = cluster.head.getFile("id_rsa")
    cluster.head.ssh(_.execute("rm -f id_rsa*"))
    for (node <- cluster) {
      node.ssh { sh =>
        sh.execute("rm -rf .ssh")
        sh.execute("mkdir .ssh")
        sh.execute("echo 'StrictHostKeyChecking no' > .ssh/config")
      }
    }
    modifyRemoteFile(cluster, ".ssh", false, false, publicKeyFile, privateKeyFile)
    for (node <- cluster) {
      node.ssh { sh =>
        sh.cd(".ssh")
        sh.execute("chmod 600 authorized_keys id_rsa")
      }
    }
  }

  def disableFirewall(cluster: Seq[SSHNode]): Unit = {
    for (node <- cluster) {
      val commands = getLinuxDistributionAndVersion(node) match {
        case ("centos", version) =>
          Seq(
            "setenforce 0",
            "sed -i 's#SELINUX=.*#SELINUX=disabled#' /etc/selinux/config") ++ {
            if (version.startsWith("7")) {
              Seq("systemctl stop firewalld.service", "systemctl disable firewalld.service")
            } else {
              Seq("service iptables stop", "chkconfig iptables off")
            }
          }
        case ("ubuntu", _) => Seq("ufw disable")
      }
      node.ssh { sh =>
        commands.foreach(sh.sudo)
      }
    }
  }

  def installJDK(cluster: Seq[SSHNode]): Unit = {
    val config =
      """
        |export JAVA_HOME=/usr/local/jdk
        |PATH=$JAVA_HOME/bin:$PATH
      |""".stripMargin
    modifyRemoteFile(cluster, "/etc", true, true, generateTempFile("profile", config))
    unpackSoftware(
      cluster,
      "jdk",
      sh => ()
    )
  }

  def installHadoop(cluster: Seq[SSHNode], master: SSHNode, secondMaster: SSHNode, dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val slaves = cluster.map(_.hostname).mkString("\n")
    val coreSite =
      s"""
        |<configuration>
        |  <property>
        |    <name>fs.defaultFS</name>
        |    <value>hdfs://${master.hostname}:9000</value>
        |  </property>
        |  <property>
        |    <name>io.file.buffer.size</name>
        |    <value>131072</value>
        |  </property>
        |</configuration>
      |""".stripMargin
    val hdfsSite =
      s"""
        |<configuration>
        |  <property>
        |    <name>dfs.replication</name>
        |    <value>3</value>
        |  </property>
        |  <property>
        |    <name>dfs.permissions.enabled</name>
        |    <value>false</value>
        |  </property>
        |  <property>
        |    <name>dfs.namenode.secondary.http-address</name>
        |    <value>${secondMaster.hostname}:50090</value>
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
      |""".stripMargin

    val configDir = "/usr/local/hadoop/etc/hadoop"
    unpackSoftware(cluster, "hadoop", sh => {
      prepareDataDir(dataDirName, sh)
      sh.cd(configDir)
      sh.execute("""sed -i "25s#\${JAVA_HOME}#$JAVA_HOME#" hadoop-env.sh""")
    })

    val slavesFile = generateTempFile("slaves", slaves)
    val coreSiteFile = generateTempFile("core-site.xml", xmlHeader + coreSite)
    val hdfsSiteFile = generateTempFile("hdfs-site.xml", xmlHeader + hdfsSite)
    modifyRemoteFile(cluster, configDir, false, false, slavesFile, coreSiteFile, hdfsSiteFile)

    master.ssh { sh =>
      import sh._
      cd("/usr/local/hadoop")
      execute("bin/hdfs namenode -format")
      execute("sbin/start-dfs.sh")
      execute("bin/hdfs dfsadmin -safemode leave")
    }
  }

  def installZooKeeper(cluster: Seq[SSHNode], dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    val serverNames = cluster.map(_.hostname)
    val configDir = "/usr/local/zookeeper/conf"
    unpackSoftware(cluster, "zookeeper", sh => {
      prepareDataDir(dataDirName, sh)
      import sh._
      cd(configDir)
      execute("mv zoo_sample.cfg zoo.cfg")
      execute(s"sed -i '12s#/tmp/zookeeper#$dataDirName#' zoo.cfg")
      execute(s"echo '${serverNames.indexOf(sh.hostname) + 1}' > $dataDir/myid")
    })
    val serversConfig = {
      for ((name, index) <- serverNames.zipWithIndex) yield s"server.${index + 1}=$name:2888:3888"
    }.mkString("\n")
    val serversConfigFile = generateTempFile("zoo.cfg", serversConfig)
    modifyRemoteFile(cluster, configDir, true, false, serversConfigFile)
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
      zooKeeperCluster: Seq[SSHNode]): Unit = {
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
        |    <name>hbase.zookeeper.quorum</name>
        |    <value>${zooKeeperCluster.map(_.hostname).mkString(",")}</value>
        |  </property>
        |  <property>
        |    <name>hbase.zookeeper.property.dataDir</name>
        |    <value>${zooKeeperDataDir.toAbsolutePath.toString}</value>
        |  </property>
        |</configuration>
      |""".stripMargin
    val configDir = "/usr/local/hbase/conf"
    unpackSoftware(cluster, "hbase", sh => {
      import sh._
      cd(configDir)
      execute("echo 'export HBASE_MANAGES_ZK=false' >> hbase-env.sh")
      execute("""echo "export JAVA_HOME=$JAVA_HOME" >> hbase-env.sh""")
    })
    val regionServersFile = generateTempFile("regionservers", cluster.map(_.hostname).mkString("\n"))
    val hbaseSiteFile = generateTempFile("hbase-site.xml", xmlHeader + hbaseSite)
    modifyRemoteFile(cluster, configDir, false, false, regionServersFile, hbaseSiteFile)
    master.ssh { sh =>
      sh.cd("/usr/local/hbase")
      sh.execute("bin/start-hbase.sh")
    }
  }

  def installKafka(cluster: Seq[SSHNode], zooKeeperCluster: Seq[SSHNode]): Unit = {
    val serverNames = cluster.map(_.hostname)
    val configDir = "/usr/local/kafka/config"
    unpackSoftware(cluster, "kafka", sh => {
      import sh._
      cd(configDir)
      execute(s"sed -i '20s#.*#broker.id=${serverNames.indexOf(sh.hostname)}#' server.properties")
      execute(s"sed -i '114d' server.properties")
    })
    val zkConfig = "zookeeper.connect=" + zooKeeperCluster.map(_.hostname + ":2181").mkString(",")
    val zkConfigFile = generateTempFile("server.properties", zkConfig)
    modifyRemoteFile(cluster, configDir, true, false, zkConfigFile)

    for (node <- cluster) {
      node.ssh { sh =>
        sh.cd("/usr/local/kafka")
        sh.execute("bin/kafka-server-start.sh -daemon config/server.properties")
      }
    }
  }

  def installSpark(cluster: Seq[SSHNode], master: SSHNode, dataDir: Path): Unit = {
    val dataDirName = dataDir.toAbsolutePath.toString
    unpackSoftware(cluster, "spark", prepareDataDir(dataDirName, _))
    val slaves = cluster.map(_.hostname).mkString("\n")
    val slavesFile = generateTempFile("slaves", slaves)
    val sparkDefaults =
      s"""
         |spark.master ${master.hostname}
         |spark.eventLog.enabled true
         |spark.eventLog.dir hdfs:///spark-event-log
       |""".stripMargin
    val sparkDefaultsFile = generateTempFile("spark-defaults.conf", sparkDefaults)
    val sparkEnv =
      s"""
        |JAVA_HOME=/usr/local/jdk
        |HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
        |SPARK_LOCAL_DIRS=$dataDirName
      |""".stripMargin
    val sparkEnvFile = generateTempFile("spark-env.sh", sparkEnv)
    modifyRemoteFile(cluster, "/usr/local/spark/conf", false, false, slavesFile, sparkDefaultsFile, sparkEnvFile)
    master.ssh(_.execute("/usr/local/spark/sbin/start-all.sh"))
  }

  def getLinuxDistributionAndVersion(node: SSHNode) = {
    val platform = """Linux.*-with-(\w+)-(\d[0-9.]*).*""".r
    node.ssh(_.executeAndTrim("python -mplatform")) match {
      case platform(distribution, version) => distribution.toLowerCase -> version
    }
  }

  private def prepareDataDir(dataDirName: String, sh: SSHShell): Unit = {
    sh.sudo(s"rm -rf $dataDirName")
    sh.sudo(s"mkdir -p $dataDirName")
    sh.sudo(s"chown ${sh.username}.${sh.username} $dataDirName")
    sh.sudo(s"chmod 775 $dataDirName")
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
      needRoot: Boolean,
      localFiles: Path*): Unit = {
    for (localFile <- localFiles) {
      val localFileName = localFile.getFileName.toString
      val remoteDestination = s"$remoteDir/$localFileName"
      val command = s"cat $localFileName ${if(isAppend) ">>" else ">"} $remoteDestination"
      for (node <- cluster) {
        node.sendFile(localFile)
        node.ssh { sh =>
          if (needRoot) {
            sh.sudo(command)
          } else {
            sh.execute(command)
          }
          sh.rm(localFileName)
        }
      }
      Files.delete(localFile)
    }
  }

  private def unpackSoftware(
              cluster: Seq[SSHNode],
              keyword: String,
              withSh: SSHShell => Unit): Unit = {
    val softwareDirName = "software"
    def getSoftwareFileName() = {
      import scala.collection.JavaConverters._
      val stream = Files.newDirectoryStream(Paths.get(softwareDirName))
      stream.iterator.asScala
        .filter(Files.isRegularFile(_))
        .map(_.getFileName.toString.toLowerCase)
        .filter(_.contains(keyword))
        .minBy(_.size)
    }

    val softwareFileName = getSoftwareFileName()
    for (node <- cluster) {
      println(s"install $keyword to ${node.hostname} ...")
      node.sendFile(Paths.get(s"$softwareDirName/$softwareFileName"))
      node.ssh { sh =>
        sh.execute(s"tar xzf $softwareFileName -C $deployDir")
        val softwareDirPath = deployDir + "/" +
          sh.executeAndTrimSplit(s"ls -t $deployDir|cat").find(_.contains(keyword)).get
        sh.sudo(s"chown -R ${sh.username}.${sh.username} $softwareDirPath")
        sh.sudo(s"chmod -R 775 $softwareDirPath")
        sh.sudo(s"rm -f /usr/local/$keyword")
        sh.sudo(s"ln -s $softwareDirPath /usr/local/$keyword")
        sh.rm(softwareFileName)
        withSh(sh)
      }
      println(s"install $keyword to ${node.hostname} done")
    }
  }


  private val xmlHeader =
    """|<?xml version="1.0" encoding="UTF-8"?>
       |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    """.stripMargin
}
