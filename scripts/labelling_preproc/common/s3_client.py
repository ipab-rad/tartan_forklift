import abc
import boto3
import json
from botocore.config import Config
from segments import SegmentsClient, exceptions

from typing import BinaryIO


class TartanAsset:

    def __init__(self, url='', uuid=''):
        self.url = url
        self.uuid = uuid


class S3Client(abc.ABC):
    """
    Abstract base class for asset uploaders.
    """

    @abc.abstractmethod
    def upload_file(self, file: BinaryIO, file_key: str) -> TartanAsset:
        """
        Upload an asset and return the access URL.
        :param file_path: Local path to the file.
        :param file_key: Destination key in S3.
        :return: URL of the uploaded asset.
        """
        pass


class SegmentS3Client(S3Client):
    """
    Uploader for SegmentsAI S3.
    """

    def __init__(self, api_key):
        self.s3_client = SegmentsClient(api_key)

    def upload_file(self, file: BinaryIO, file_key: str) -> TartanAsset:
        """
        Uploads a file to a SegmemtsAI's S3
        """
        segment_asset = self.s3_client.upload_asset(file, file_key)

        asset = TartanAsset(segment_asset.url, segment_asset.uuid)

        return asset

    def print_datasets(self):
        datasets = self.s3_client.get_datasets()
        for dataset in datasets:
            print(dataset.name, dataset.description)

    def verify_dataset(self, dataset_name: str) -> None:
        """
        Verify that a dataset exists.

        :param dataset_name: The name of the dataset to verify.
        :raises exceptions.ValidationError: If dataset validation fails.
        :raises exceptions.APILimitError: If the API limit is exceeded.
        :raises exceptions.NotFoundError: If the dataset is not found.
        :raises exceptions.TimeoutError: If the request times out.
        """
        try:
            self.s3_client.get_dataset(dataset_name)
        except exceptions.ValidationError as e:
            raise exceptions.ValidationError(
                f'Failed to validate \'{dataset_name}\' dataset.'
            ) from e
        except exceptions.APILimitError as e:
            raise exceptions.APILimitError('API limit exceeded.') from e
        except exceptions.NotFoundError as e:
            raise exceptions.NotFoundError(
                f'Dataset \'{dataset_name}\' does not exist. Please provide an existent dataset.'
            ) from e
        except exceptions.TimeoutError as e:
            raise exceptions.TimeoutError(
                'Request timed out. Try again later.'
            ) from e

    def add_sample(
        self, dataset_name: str, sequence_name: str, attributes: dict
    ) -> None:
        """
        Add a sample to a SegmentsAI dataset.

        :param dataset_name: The name of the dataset.
        :param sequence_name: The sequence name within the dataset.
        :param attributes: A dictionary containing sample attributes.
        :return: The created sample object.
        :raises exceptions.ValidationError: If sample validation fails.
        :raises exceptions.APILimitError: If the API limit is exceeded.
        :raises exceptions.NotFoundError: If the dataset is not found.
        :raises exceptions.NetworkError: If a network error occurs.
        :raises exceptions.TimeoutError: If the request times out.
        """
        try:
            self.s3_client.add_sample(dataset_name, sequence_name, attributes)
        except exceptions.ValidationError as e:
            raise exceptions.ValidationError(
                'Failed to validate sample.'
            ) from e
        except exceptions.APILimitError as e:
            raise exceptions.APILimitError('API limit exceeded.') from e
        except exceptions.NotFoundError as e:
            raise exceptions.NotFoundError(
                f'Dataset \'{dataset_name}\' does not exist. Please provide an existent dataset.'
            ) from e
        except exceptions.AlreadyExistsError as e:
            raise exceptions.AlreadyExistsError(
                f'The sequence \'{sequence_name}\' already exists in \'{dataset_name}\''
            ) from e
        except exceptions.TimeoutError as e:
            raise exceptions.TimeoutError(
                'Request timed out while adding sample.'
            ) from e


class EIDFfS3Client(S3Client):
    """
    Uploader for EIDF S3
    """

    def __init__(self, project_name: str, bucket_name: str, endpoint_url: str):
        # Needed as per EIDF instructions
        config = Config(
            request_checksum_calculation='when_required',
            response_checksum_validation='when_required',
        )

        self.s3_client = boto3.resource('s3', config=config)
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url

    def upload_file(self, file: BinaryIO, file_key: str) -> TartanAsset:
        """
        Uploads a file to EIDF S3 and returns a S3 URL
        """
        response = self.s3_client.Bucket(self.bucket_name).put_object(
            Key=file_key, Body=file
        )

        if response:
            asset = TartanAsset(
                f'{self.endpoint_url}/{self.project_name}%3A{self.bucket_name}/{file_key}',
                'super_unique_id',
            )
            return asset

        return None

    def print_object_list(self, max_prints=None):
        bucket = self.s3_client.Bucket(self.bucket_name)

        for idx, obj in enumerate(bucket.objects.all()):
            print(f'Obj {idx}: {obj.key}')
            print(
                f'\tURL: {self.endpoint_url}/{self.project_name}%3A{self.bucket_name}/{obj.key}'
            )
            if max_prints is not None:
                if idx > max_prints:
                    break

    def set_bucket_policy(self, policy_dict):
        bucket_policy = self.s3_client.Bucket(self.bucket_name).Policy()
        bucket_policy.put(Policy=json.dumps(policy_dict))
