package deploy.util

import java.nio.file.{Paths, Path}

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-22.
 */
case class SSHNode(ip: String, hostname: String, username: String, password: String) {
  def ssh[T](withsh: SSHShell => T): T = jassh.SSH.shell(ip, username, password)(withsh)

  def sendFile(localPath: Path, remotePath: String): Unit = {
    jassh.SSH.once(ip, username, password) { ssh =>
      ssh.rm(remotePath)
      ssh.send(localPath.toAbsolutePath.toString, remotePath)
    }
  }

  def sendFile(localPath: Path): Unit = sendFile(localPath, localPath.getFileName.toString)

  def getFile(remotePath: String, localPath: Path): Path = {
    jassh.SSH.once(ip, username, password) { ssh =>
      ssh.receive(remotePath, localPath.toAbsolutePath.toString)
    }
    localPath
  }

  def getFile(remotePath: String, localFileName: String): Path = getFile(remotePath, Paths.get(localFileName))

  def getFile(remotePath: String): Path = getFile(remotePath, Paths.get(remotePath).getFileName.toString)
}
