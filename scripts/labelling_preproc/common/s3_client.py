"""Module to define S3 client interfaces for different S3 organisations."""

import abc
import json
from typing import BinaryIO, Optional

import boto3

from botocore.config import Config

from segments import SegmentsClient


class TartanAsset:
    """Class to represent asset S3 URL and UUID."""

    def __init__(self, url='', uuid=''):
        """
        Initialise the asset representation.

        Args:
            url: The S3 URL of the asset.
            uuid: The UUID of the asset.
        """
        self.url = url
        self.uuid = uuid


class S3Client(abc.ABC):
    """Abstract base class for an S3 client."""

    @abc.abstractmethod
    def upload_file(self, file: BinaryIO, file_key: str) -> TartanAsset:
        """
        Upload an asset and return its access URL and UUID.

        Args:
            file: File-like object to upload.
            file_key: Destination key in S3.

        Returns:
            A TartanAsset containing the URL and UUID.
        """
        pass


class SegmentS3Client(S3Client):
    """Class to interface with Segments.ai S3 and platform."""

    def __init__(self, api_key: str):
        """
        Initialise the Segments client with an API key.

        Args:
            api_key: Segments.ai API key.
        """
        self.s3_client = SegmentsClient(api_key)

    def upload_file(self, file: BinaryIO, file_key: str) -> TartanAsset:
        """
        Upload a file to Segments.ai S3.

        Args:
            file: File-like object to upload.
            file_key: Destination key in Segments.ai S3.

        Returns:
            A TartanAsset containing the URL and UUID.
        """
        segment_asset = self.s3_client.upload_asset(file, file_key)
        asset = TartanAsset(segment_asset.url, segment_asset.uuid)
        return asset


class EIDFfS3Client(S3Client):
    """Class to interface with EIDF S3."""

    def __init__(self, project_name: str, bucket_name: str, endpoint_url: str):
        """
        Initialise the EIDF S3 client.

        Args:
            project_name: Project identifier.
            bucket_name: Name of the S3 bucket.
            endpoint_url: S3 endpoint url.
        """
        config = Config(
            request_checksum_calculation='when_required',
            response_checksum_validation='when_required',
        )
        self.s3_client = boto3.resource('s3', config=config)
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url

    def upload_file(
        self, file: BinaryIO, file_key: str
    ) -> Optional[TartanAsset]:
        """
        Upload a file to EIDF S3 and return its access URL.

        Args:
            file: File binary object to upload.
            file_key: Destination key in S3.

        Returns:
            A TartanAsset object or None if upload fails.
        """
        response = self.s3_client.Bucket(self.bucket_name).put_object(
            Key=file_key, Body=file
        )
        if response:
            s3_url = (
                f'{self.endpoint_url}/{self.project_name}%3A'
                f'{self.bucket_name}/{file_key}'
            )
            asset = TartanAsset(s3_url, 'super_unique_id')
            return asset
        return None

    def print_object_list(self, max_prints: Optional[int] = None) -> None:
        """
        List all objects in the S3 bucket.

        Args:
            max_prints: Optional limit on the number of objects to print.
        """
        bucket = self.s3_client.Bucket(self.bucket_name)
        for idx, obj in enumerate(bucket.objects.all()):
            s3_url = (
                f'{self.endpoint_url}/{self.project_name}%3A'
                f'{self.bucket_name}/{obj.key}'
            )
            print(f'Obj {idx}: {obj.key}')
            print(f'\tURL: {s3_url}')
            if max_prints is not None and idx > max_prints:
                break

    def set_bucket_policy(self, policy_dict: dict) -> None:
        """
        Set a bucket policy using a JSON dictionary.

        Args:
            policy_dict: Dictionary representing the bucket policy.
        """
        bucket_policy = self.s3_client.Bucket(self.bucket_name).Policy()
        bucket_policy.put(Policy=json.dumps(policy_dict))
