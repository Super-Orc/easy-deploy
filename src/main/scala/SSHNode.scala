import fr.janalyse.ssh.{SSHShell, SSH}

/**
 * Created by cloud on 14-10-22.
 */
case class SSHNode(host: String, username: String, password: String) {

  def once(withssh: SSH => Unit): Unit = {
    jassh.SSH.once(host, username, password)(withssh)
  }

  def shell(withsh: SSHShell => Unit): Unit = {
    jassh.SSH.shell(host, username, password)(withsh)
  }
}
