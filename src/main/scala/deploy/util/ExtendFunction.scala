package deploy.util

import fr.janalyse.ssh.SSHShell

/**
 * Created by cloud0fan on 11/19/14.
 */
object ExtendFunction {
  implicit class ExtendedSSHShell(sh: SSHShell) {
    def sudo(command: String) = {
      sh.execute(s"""echo $password | sudo -k -S sh -c "$command" """)
    }

    def username = sh.options.username

    def password = sh.options.password
  }
}
