"""Module to define S3 client interfaces for different S3 organisations."""

import abc
from typing import BinaryIO, Optional

import boto3
from boto3.s3.transfer import TransferConfig

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

    # FIXME: Hardcode the multipart parameters for now (#70)
    def __init__(
        self,
        project_name: str,
        bucket_name: str,
        endpoint_url: str,
        multipart_threshold_GB: int = 3,
        _max_concurrency: int = 4,
    ):
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

        # Configure S3 TransferConfig with multipart support
        GB_TO_BYTES = 1024**3
        multipart_threshold_bytes = multipart_threshold_GB * GB_TO_BYTES
        print(f'Setting multipart threshold to {multipart_threshold_GB} GB')
        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold_bytes,
            max_concurrency=_max_concurrency,
        )

        self.s3_client = boto3.client('s3', config=config)
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
        try:
            self.s3_client.upload_fileobj(
                file, self.bucket_name, file_key, Config=self.transfer_config
            )
        except Exception as e:  # noqa: B902
            # Catch all exceptions to handle upload errors
            print(f'Error uploading file to S3: {e}')
            return None

        s3_url = (
            f'{self.endpoint_url}/{self.project_name}%3A'
            f'{self.bucket_name}/{file_key}'
        )
        # TODO: Do we need an uuid at all? #69
        asset = TartanAsset(s3_url, 'super_unique_id')
        return asset
