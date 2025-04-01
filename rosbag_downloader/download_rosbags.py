import argparse
import os
import subprocess
import os
from pathlib import Path
import yaml
import time

from ftplib import FTP


class RosbagsDownloader:

    def __init__(self, config: dict, use_ftp: bool, output_directory: str):
        self.remote_user = config['remote_user']
        self.remote_hostname = config['remote_ip']
        self.remote_directory = config['remote_directory']
        self.host_directory = config['cloud_upload_directory']
        
        # Override host directory if another output directory is provided
        if output_directory != '':
            self.host_directory = output_directory

        print(f'Using output directory: {self.host_directory}')
        transfer_method = 'FTP' if use_ftp else 'rsync'
        print(f'Using {transfer_method} for file transfer')
        
        self.files_size_dict = {}
        
        self.ftp = None
        self.use_ftp = use_ftp
        if use_ftp:
            try:
                self.ftp = FTP(self.remote_hostname)
                pwd = 'mypwd'
                self.ftp.login(user=self.remote_user, passwd=pwd)

                # Change to the desired directory
                self.ftp.cwd(self.remote_directory)

            except Exception as e:
                print(f'[✖] FTP connection failed: {e}')

    def ftp_file(self, file_path):

        relative_file_path = os.path.relpath(
            file_path, start=self.remote_directory
        )
        host_destination = os.path.join(
            self.host_directory, relative_file_path
        )

        # Make sure the local directory exists
        os.makedirs(os.path.dirname(host_destination), exist_ok=True)

        try:
            print(f'[+] Starting FTP transfer: {relative_file_path}')
            with open(host_destination, 'wb') as f:
                self.ftp.retrbinary(f'RETR {relative_file_path}', f.write)

        except Exception as e:
            print(f'[✖] FTP upload failed for {relative_file_path}: {e}')

    def rsync_file(self, file_path):
        remote_file = f'{self.remote_user}@{self.remote_hostname}:{file_path}'
        relative_file_path = os.path.relpath(
            file_path, start=self.remote_directory
        )
        host_destination = os.path.join(
            self.host_directory, relative_file_path
        )

        os.makedirs(os.path.dirname(host_destination), exist_ok=True)

        cmd = [
            'rsync',
            '-a',
            '--no-i-r',
            '--info=progress2',
            '--whole-file',
            '--no-compress',
            remote_file,
            host_destination,
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f'[✖] Failed: {file_path}\n{e}')

    def get_remote_file_size(self, remote_abs_file_path) -> int:
        ssh_target = f'{self.remote_user}@{self.remote_hostname}'
        cmd = ['ssh', ssh_target, f'stat -c%s {remote_abs_file_path}']

        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            return int(output.decode().strip())
        except subprocess.CalledProcessError as e:
            print(f'[Error] SSH command failed: {e.output.decode().strip()}')
            return None
        except ValueError:
            print('[Error] Failed to parse file size from SSH output.')
            return None

    def get_remote_files_sizes(self) -> dict:
        '''Get the sizes of all files in a remote directory.'''
        ssh_target = f'{self.remote_user}@{self.remote_hostname}'
        find_cmd = f'find {self.remote_directory} -type f -name \'*.mcap\''
        cmd = ['ssh', ssh_target, find_cmd]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            files = output.decode().strip().split('\n')
            files_size_dict = {}
            for file in files:
                file = file.strip()
                if file:
                    files_size_dict[file] = self.get_remote_file_size(file)
            return files_size_dict
        except subprocess.CalledProcessError as e:
            print(f'[Error] SSH command failed: {e.output.decode().strip()}')
            return []

    def get_rosbags_files(self, directory: Path):
        # Recursively find all .mcap files
        mcap_files = list(directory.rglob('*.mcap'))
        # Convert to string paths if needed
        mcap_file_paths = [str(path) for path in mcap_files]
        return mcap_file_paths

    def get_remote_rosbags_list(self, remote_directory):
        '''Get a list of all rosbags on the remote machine.'''
        list_cmd = f'find {remote_directory} -name \'*.mcap\' | sort -V'
        try:
            result = subprocess.run(
                f'ssh {self.remote_user}@{self.remote_hostname} \'{list_cmd}\'',
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            rosbag_list = result.stdout.splitlines()
            return rosbag_list
        except subprocess.CalledProcessError as e:
            print(f'Failed to list rosbags on remote machine: {e}')
            raise

    def get_remote_directories(self, remote_directory):
        '''List directories on the remote machine containing .mcap files.'''
        list_cmd = (
            f'ssh {self.remote_user}@{self.remote_hostname} '
            f'"find {remote_directory} -type f -name \'*.mcap\' -printf \'%h\\n\' | sort -u"'
        )
        try:
            result = subprocess.run(
                list_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            subdirectories = result.stdout.splitlines()
            return subdirectories
        except subprocess.CalledProcessError as e:
            print(f'Failed to list directories in {remote_directory}: {e}')
            return []

    def main(self, number_workers) -> None:
        print(
            f'Searching for .mcap files in:\n\t{self.remote_user}@{self.remote_hostname}:{self.remote_directory}'
        )
        rosbags_directories = self.get_remote_directories(
            self.remote_directory
        )
        print(
            f'Found {len(rosbags_directories)} directories containing rosbags files:'
        )
        print('\n'.join(rosbags_directories))

        print('Extracting file sizes...')
        self.files_size_dict = self.get_remote_files_sizes()
        for key, value in self.files_size_dict.items():
            print(f'{key}: {value}')

        counter = 0
        max_downloads = 5
        exit = False

        total_downloading_time = 0
        total_files_size = 0
        for rosbag_directory in rosbags_directories:
            print(f'\nProcessing directory: {rosbag_directory}')
            rosbags_list = self.get_remote_rosbags_list(rosbag_directory)
            print(f'\tFound {len(rosbags_list)} rosbags in the directory')

            for rosbag in rosbags_list:
                if counter >= max_downloads:
                    print(f'Max downloads reached: {max_downloads}')
                    exit = True
                    break
                print(f'Downloading {rosbag}...')
                start_time = time.time()
                if self.use_ftp:
                    self.ftp_file(rosbag)
                else:  
                    self.rsync_file(rosbag)
                elapsed_time = time.time() - start_time
                total_downloading_time += elapsed_time

                rosbag_size = self.files_size_dict[rosbag]
                total_files_size += rosbag_size
                # Compute transfer speed in MB/s
                avg_transfer_speed = (
                    rosbag_size / (1024 * 1024)
                ) / elapsed_time
                print(
                    f'\t File transferred in {elapsed_time:.2f} sec at {avg_transfer_speed:.2f} MB/s (average)\n'
                )
                counter += 1

            if exit:
                break

        avg_transfer_speed = (
            total_files_size / (1024 * 1024)
        ) / total_downloading_time
        print(
            f'✅ {counter} rosbags were uploaded in {total_downloading_time:.2f} sec at {avg_transfer_speed:.2f} MB/s (average)'
        )

        if hasattr(self, 'ftp') and self.ftp:
            try:
                self.ftp.quit()
            except Exception as e:
                print(f'Error closing FTP session: {e}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Automate the compression and upload of rosbags.'
    )
    parser.add_argument(
        '-c',
        '--config',
        default='vehicle_data_params.yaml',
        help='Path to the YAML configuration file',
    )
    parser.add_argument(
        '--use-ftp',
        action='store_true',
        help='Whether to use FTP or rsync for file transfer',
    )
    parser.add_argument(
        '-o',
        '--output',
        default='',
        help='Output directory to save the downloaded files',
    )    
    parser.add_argument(
        '-n',
        '--parallel-workers',
        default=1,
        help='Number of workers to do the parallel sending',
    )
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Enable debugging prints',
    )
    args = parser.parse_args()

    with open(args.config) as file:
        config = yaml.safe_load(file)

    send_tester = RosbagsDownloader(config, args.use_ftp, args.output)
    send_tester.main(int(args.parallel_workers))
