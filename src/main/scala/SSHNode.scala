import java.nio.file.Path

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud on 14-10-22.
 */
case class SSHNode(host: String, username: String, password: String) {

  def ssh(withsh: SSHShell => Unit): Unit = {
    jassh.SSH.shell(host, username, password)(withsh)
  }

  def sendFile(fromLocalFile: Path, remoteDestination: String): Unit = {
    println(s"$host: receiving file ${fromLocalFile.getFileName}...")
    jassh.SSH.once(host, username, password) { ssh =>
      ssh.rm(remoteDestination)
      ssh.send(fromLocalFile.toString, remoteDestination)
    }
    println(s"$host: finished")
  }

  def sendFile(fromLocalFile: Path): Unit = sendFile(fromLocalFile, fromLocalFile.toString)
}
