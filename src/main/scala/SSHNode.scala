import java.nio.file.Path

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-22.
 */

trait SSHNode {
  def host: String
  def username: String
  def password: String

  def ssh(withsh: SSHShell => Unit): Unit = {
    jassh.SSH.shell(host, username, password)(withsh)
  }

  def sshWithRootShell(withsh: SSHShell => Unit) = ssh(withsh)

  def sendFile(fromLocalFile: Path, remoteDestination: String): Unit = {
    println(s"$host: receiving file '${fromLocalFile.getFileName}' ...")
    jassh.SSH.once(host, username, password) { ssh =>
      ssh.rm(remoteDestination)
      ssh.send(fromLocalFile.toString, remoteDestination)
    }
    println(s"$host: finished")
  }

  def sendFile(fromLocalFile: Path): Unit = sendFile(fromLocalFile, fromLocalFile.getFileName.toString)
}

case class NormalNode(host: String, username: String, password: String, rootPassword: String) extends SSHNode {
  override def sshWithRootShell(withsh: SSHShell => Unit): Unit = {
    val rootSh = (sh: SSHShell) => {
      sh.become("root", Some(rootPassword))
      sh.cd(s"/home/$username")
      withsh(sh)
    }
    jassh.SSH.shell(host, username, password)(rootSh)
  }
}

case class RootNode(host: String, password: String) extends SSHNode {
  def username = "root"
}
