package deploy.util

import java.nio.file.Path

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-22.
 */
case class SSHNode(ip: String, host: String, username: String, password: String) {
  def ssh[T](withsh: SSHShell => T): T = {
    jassh.SSH.shell(ip, username, password)(withsh)
  }

  def sendFile(fromLocalFile: Path, remoteDestination: String): Unit = {
    jassh.SSH.once(ip, username, password) { ssh =>
      ssh.rm(remoteDestination)
      ssh.send(fromLocalFile.toString, remoteDestination)
    }
  }

  def sendFile(fromLocalFile: Path): Unit = sendFile(fromLocalFile, fromLocalFile.getFileName.toString)
}
