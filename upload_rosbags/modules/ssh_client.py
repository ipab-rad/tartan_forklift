"""
SSH client module to handle remote SSH connections and commands.

Implements the `SSHClient` class to wrap the SSH connections and
command execution.
"""

from logging import Logger
from pathlib import Path

import paramiko
from paramiko import SSHConfig


from paramiko_jump import SSHJumpClient

from upload_rosbags.modules.config_parser import Parameters


class SSHClient:
    """SSH client wrapper for remote connections."""

    def __init__(self, config: Parameters, logger: Logger):
        """Initialise the SSH connection."""
        self.client = None
        if config.cloud_ssh_alias:
            resolved = self.resolve_ssh_alias(config.cloud_ssh_alias)
            hostname = resolved['hostname']
            username = resolved['username']

            # It it contains a jump
            if resolved['jump_host']:
                jump_host = resolved['jump_host']
                jump_username = resolved['jump_user']

                logger.debug(
                    f'[SSH Client] Creating a connection to '
                    f'{config.cloud_ssh_alias} through {jump_host} proxy'
                )
                # Create SSH client for the jump host
                jumper = SSHJumpClient()
                jumper.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                jumper.connect(
                    hostname=jump_host,
                    username=jump_username,
                    allow_agent=True,
                    look_for_keys=True,
                )

                # Create target client through the jump host
                self.client = SSHJumpClient(jump_session=jumper)
                self.client.set_missing_host_key_policy(
                    paramiko.AutoAddPolicy()
                )
                self.client.connect(hostname=hostname, username=username)
                logger.debug('[SSH Client] Connected')

        else:

            hostname = config.cloud_hostname
            username = config.cloud_user

            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            logger.debug(
                f'[SSH Client] Creating a connection to {username}@{hostname}'
            )

            self.client.connect(
                hostname=hostname,
                username=username,
                allow_agent=True,
                look_for_keys=True,
            )

            logger.debug('[SSH Client] Connected')

    def resolve_ssh_alias(self, alias: str) -> dict:
        """Resolve the SSH alias to a hostname and user."""
        ssh_config_file = Path.home() / '.ssh' / 'config'
        with ssh_config_file.open() as f:
            ssh_config = SSHConfig()
            ssh_config.parse(f)

        # Look up a host alias defined in your config
        host_config = ssh_config.lookup(alias)

        resolved = {}
        resolved['hostname'] = host_config.get('hostname')
        resolved['username'] = host_config.get('user')
        resolved['jump_host'] = None

        if 'proxyjump' in host_config:
            jump_config = ssh_config.lookup(host_config['proxyjump'])
            resolved['jump_host'] = jump_config.get('hostname')
            resolved['jump_user'] = jump_config.get('user')
        elif 'proxycommand' in host_config:
            raise ValueError('[SSH Cient] ProxyCommand is not supported')

        return resolved

    def send_command(self, cmd: str):
        """Send command to the remote server and return the output."""
        stdin, stdout, stderr = self.client.exec_command(cmd)
        output = stdout.read().decode()
        output_stderr = stderr.read().decode()

        return output, output_stderr
