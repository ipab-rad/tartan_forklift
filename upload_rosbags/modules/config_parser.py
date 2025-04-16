import yaml
import os

from upload_rosbags.modules.data_types import Parameters
        
class ConfigParser:
    def __init__(self):
        self.required_params = [
            'local_host_user',
            'local_hostname',
            'local_rosbags_directory',
            'cloud_upload_directory',
            'upload_attempts',
            'mcap_path',
            'mcap_compression_chunk_size',
            'compression_parallel_workers',
            'compression_queue_max_size'
        ]
    
    
    def load_config(self, config_path) -> Parameters:
        """
        Load the configuration from a YAML file.
        
        :param config_path: Path to the YAML configuration file
        :return: Parsed configuration as a dictionary
        """
        
        with open(config_path, 'r') as file:
            yaml_config = yaml.safe_load(file)
            
            self.validate_yaml_config(yaml_config)
            
            parameters = Parameters(
                local_host_user = yaml_config.get('local_host_user'),
                local_hostname = yaml_config.get('local_hostname'),
                local_rosbags_directory = yaml_config.get('local_rosbags_directory'),
                cloud_user = yaml_config.get('cloud_user'),
                cloud_hostname = yaml_config.get('cloud_hostname'),
                cloud_ssh_alias = yaml_config.get('cloud_ssh_alias'),
                cloud_upload_directory = yaml_config.get('cloud_upload_directory'),
                mcap_bin_path = yaml_config.get('mcap_bin_path'),
                mcap_compression_chunk_size = int(yaml_config.get('mcap_compression_chunk_size')),
                compression_parallel_workers = int(yaml_config.get('compression_parallel_workers')),
                compression_queue_max_size = int(yaml_config.get('compression_queue_max_size'))
            )
            return parameters


    def validate_yaml_config(self, yaml_config: dict) -> None:
        """
        Validates the provided YAML configuration dictionary.
        Ensures all required parameters are present and meet the expected
        types and constraints. Raises exceptions if validation
        fails.
        Args:
            yaml_config (dict): The YAML configuration to validate.
        Raises:
            KeyError: If required parameters are missing.
            ValueError: If parameters have invalid types or values.
        """
        
        ssh_alias_defined = bool(yaml_config.get('cloud_ssh_alias', '').strip())
        
        # Adjust required parameters keys
        if not ssh_alias_defined:
            self.required_params.append('cloud_user')
            self.required_params.append('cloud_hostname')
        
        missing_keys = [key for key in self.required_params if key not in yaml_config]
        if missing_keys:
            raise KeyError(
                f'Missing YAML configuration parameters: {missing_keys}'
            )
            
        # Local host params
        if (
            not isinstance(yaml_config['local_host_user'], str)
            or not yaml_config['local_host_user'].strip()
        ):
            raise ValueError('"local_host_user" must be a string and non-empty.')

        if not yaml_config['local_hostname'].strip():
            raise ValueError('"local_hostname" must be non-empty.')

        if not os.path.isabs(yaml_config['local_rosbags_directory']):
            raise ValueError('"local_rosbags_directory" must be an absolute path.')
        
        # Cloud params
        if not ssh_alias_defined:
            if (
                not isinstance(yaml_config['cloud_user'], str)
                or not yaml_config['cloud_user'].strip()
            ):
                raise ValueError('"cloud_user" must be a string and non-empty if "cloud_ssh_alias" is not defined.')
            
            if not yaml_config['cloud_hostname'].strip():
                raise ValueError('"cloud_hostname" must be non-empty if "cloud_ssh_alias" is not defined.')

        if not os.path.isabs(yaml_config['cloud_upload_directory']):
            raise ValueError('"cloud_upload_directory" must be an absolute path.')


        # Other params
        if (
            not isinstance(yaml_config['upload_attempts'], int)
            or yaml_config['upload_attempts'] <= 0
        ):
            raise ValueError('"upload_attempts" must be a positive integer.')
        
        if not os.path.isabs(yaml_config['mcap_path']):
            raise ValueError('"mcap_path" must be an absolute path.')

        if (
            not isinstance(yaml_config['mcap_compression_chunk_size'], int)
            or yaml_config['mcap_compression_chunk_size'] <= 0
        ):
            raise ValueError(
                '"mcap_compression_chunk_size" must be a positive integer.'
            )

        if (
            not isinstance(yaml_config['compression_parallel_workers'], int)
            or yaml_config['compression_parallel_workers'] <= 0
        ):
            raise ValueError('"compression_parallel_workers" must be a positive integer.')
        
        if (
            not isinstance(yaml_config['compression_queue_max_size'], int)
            or yaml_config['compression_queue_max_size'] <= 0
        ):
            raise ValueError('"compression_queue_max_size" must be a positive integer.')