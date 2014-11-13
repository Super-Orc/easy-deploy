package deploy

import java.nio.file.Path

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-22.
 */

trait SSHNode {
  def ip: String
  def host: String
  def username: String
  def password: String

  def ssh[T](withsh: SSHShell => T): T = {
    jassh.SSH.shell(ip, username, password)(withsh)
  }

  def sshWithRootShell[T](withsh: (SSHShell, String) => T): T = ssh(withsh(_, username))

  def sshWithRootShell[T](withsh: SSHShell => T): T = sshWithRootShell{(sh, _) => withsh(sh)}

  def sendFile(fromLocalFile: Path, remoteDestination: String): Unit = {
    jassh.SSH.once(ip, username, password) { ssh =>
      ssh.rm(remoteDestination)
      ssh.send(fromLocalFile.toString, remoteDestination)
    }
  }

  def sendFile(fromLocalFile: Path): Unit = sendFile(fromLocalFile, fromLocalFile.getFileName.toString)
}

case class NormalNode(ip: String, host: String, username: String, password: String, rootPassword: String) extends SSHNode {
  override def sshWithRootShell[T](withsh: (SSHShell, String) => T): T = {
    val rootSh = (sh: SSHShell) => {
      sh.become("root", Some(rootPassword))
      sh.cd(s"/home/$username")
      withsh(sh, username)
    }
    ssh(rootSh)
  }
}

case class RootNode(ip: String, host: String, password: String) extends SSHNode {
  def username = "root"
}
