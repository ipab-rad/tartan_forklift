import argparse
import os
import subprocess
import os
from pathlib import Path
import yaml
import time

class RosbagsDownloader:

    def __init__(self, config: dict, use_ftp: bool, ftp_password: str,ftp_parallel_workers: int,  output_directory: str):
        self.remote_user = config['remote_user']
        self.remote_hostname = config['remote_ip']
        self.remote_directory = config['remote_directory']
        self.host_directory = config['cloud_upload_directory']
        
        # Override host directory if another output directory is provided
        if output_directory != '':
            self.host_directory = output_directory

        self.lftp_threads = ftp_parallel_workers
        self.remote_password = ftp_password

        print(f'Using output directory: {self.host_directory}')
        meta= ''
        if use_ftp:
            transfer_method = 'lftp'
            meta = f'\n\t Max num of connections: {self.lftp_threads}'
        else:
            transfer_method = 'rsync'   
        print(f'Using {transfer_method} for file transfer {meta}')
        
        self.max_downloads = 4
        self.files_size_dict = {}
        
        self.use_ftp = use_ftp


    def lftp_file(self, file_path):
        """
        Downloads a file from an FTP server using lftp and parallel segmented download.
        
        Arguments:
        - file_path: Absolute path to the file on the remote FTP server.
        """

        # Preserve local directory structure relative to remote_directory
        relative_file_path = os.path.relpath(file_path, start=self.remote_directory)
        host_destination = os.path.join(self.host_directory, relative_file_path)

        # Ensure the local directory exists
        os.makedirs(os.path.dirname(host_destination), exist_ok=True)

        command = (
            f'lftp -u "{self.remote_user},{self.remote_password}" {self.remote_hostname} '
            f'-e "pget -n {self.lftp_threads} \\"{file_path}\\" -o \\"{host_destination}\\"; bye"'
        )

        try:
            print(f"[+] Starting FTP transfer with lftp: {relative_file_path}")
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            for line in process.stdout:
                print(f"[lftp] {line.strip()}")

            process.wait()

            if process.returncode != 0:
                print(f"[✖] lftp failed with code {process.returncode}")

        except Exception as e:
            print(f"[✖] lftp download failed for {file_path}: {e}")



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

    def main(self) -> None:
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
        exit = False

        total_downloading_time = 0
        total_files_size = 0
        for rosbag_directory in rosbags_directories:
            print(f'\nProcessing directory: {rosbag_directory}')
            rosbags_list = self.get_remote_rosbags_list(rosbag_directory)
            print(f'\tFound {len(rosbags_list)} rosbags in the directory')

            for rosbag in rosbags_list:
                if counter >= self.max_downloads:
                    print(f'Max downloads reached: {self.max_downloads}')
                    exit = True
                    break
                print(f'Downloading {rosbag}...')
                start_time = time.time()
                if self.use_ftp:
                    self.lftp_file(rosbag)
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
                
                avg_trasfer_speed_gbps= avg_transfer_speed / 125.0
                
                print(
                    f'\t File transferred in {elapsed_time:.2f} sec at {avg_trasfer_speed_gbps:.2f} Gbit/s ({avg_transfer_speed:.2f} MB/s ) average \n'
                )
                counter += 1

            if exit:
                break

        avg_transfer_speed = (
            total_files_size / (1024 * 1024)
        ) / total_downloading_time
        
        avg_trasfer_speed_gbps= avg_transfer_speed / 125.0
        
        print(
            f'✅ {counter} rosbags were uploaded in {avg_trasfer_speed_gbps:.2f} Gbit/s ({avg_transfer_speed:.2f} MB/s ) average'
        )



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
        '--ftp-password',
        default='mypwd',
        help='If --use-ftp is set, the password to use for FTP connection',
    ) 
    parser.add_argument(
        '-n',
        '--ftp-parallel-workers',
        default=4,
        help='If --use-ftp is set, the number of parallel workers to use for FTP transfer',
    )
    parser.add_argument(
        '-o',
        '--output',
        default='',
        help='Output directory to save the downloaded files',
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

    send_tester = RosbagsDownloader(config, args.use_ftp, args.ftp_password, int(args.ftp_parallel_workers), args.output)
    send_tester.main()
