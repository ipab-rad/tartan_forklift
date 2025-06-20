from segments import SegmentsClient, exceptions
from labelling_preproc.common.types import SegmentsAIError, Response


class SegmentsWrrapperClient():
    """Class to interface with Segments.ai SDK"""

    def __init__(self, api_key: str):
        """
        Initialise the Segments client with an API key.

        Args:
            api_key: Segments.ai API key.
        """
        self.s3_client = SegmentsClient(api_key)

    def verify_dataset(self, dataset_name: str) -> None:
        """
        Verify that a dataset exists on Segments.ai.

        Args:
            dataset_name: The name of the dataset to verify.

        Raises:
            ValidationError: If dataset validation fails.
            APILimitError: If the API limit is exceeded.
            NotFoundError: If the dataset is not found.
            TimeoutError: If the request times out.
        """
        try:
            self.s3_client.get_dataset(dataset_name)
        except exceptions.ValidationError as e:
            raise exceptions.ValidationError(
                f'Failed to validate "{dataset_name}" dataset.'
            ) from e
        except exceptions.APILimitError as e:
            raise exceptions.APILimitError('API limit exceeded.') from e
        except exceptions.NotFoundError as e:
            raise exceptions.NotFoundError(
                f'Dataset "{dataset_name}" does not exist. '
                f'Please provide an existent dataset.'
            ) from e
        except exceptions.TimeoutError as e:
            raise exceptions.TimeoutError(
                'Request timed out. Try again later.'
            ) from e

    def add_sample(
        self, dataset_name: str, sequence_name: str, attributes: dict
    ) -> Response:
        """
        Add a sample to a Segments.ai dataset.

        Args:
            dataset_name: The name of the dataset.
            sequence_name: The sequence name within the dataset.
            attributes: A dictionary containing sample attributes.

        Raises:
            ValidationError: If sample validation fails.
            APILimitError: If the API limit is exceeded.
            NotFoundError: If the dataset is not found.
            AlreadyExistsError: If the sequence already exists.
            NetworkError:  If the request is not valid or if the server
                            experienced an error.
            TimeoutError: If the request times out.
        """
        try:
            self.s3_client.add_sample(dataset_name, sequence_name, attributes)
        except exceptions.ValidationError as e:
            message = f'Failed to validate sample: {str(e)}'
            return Response(ok=False, error=SegmentsAIError.ValidationError,
                            error_message=message)

        except exceptions.APILimitError as e:
            message = 'API limit exceeded: ' + str(e)
            return Response(ok=False, error=SegmentsAIError.APILimitError,
                            error_message=message)

        except exceptions.NotFoundError as e:
            message = (f'Dataset "{dataset_name}" does not exist'
                       ' to add a sample: ' + str(e))
            return Response(ok=False, error=SegmentsAIError.NotFoundError,
                            error_message=message)

        except exceptions.AlreadyExistsError as e:
            # raise exceptions.AlreadyExistsError(
            #     f'The sequence "{sequence_name}" '
            #     f'already exists in "{dataset_name}"'
            # ) from e
            message = (f'The sequence "{sequence_name}"'
                       f' already exists in "{dataset_name}"')
            return Response(ok=False, error=SegmentsAIError.AlreadyExistsError,
                            error_message=str(e))

        except exceptions.NetworkError as e:
            # raise exceptions.NetworkError(
            #     'Network error occurred while adding sample.'
            # ) from e
            return Response(ok=False, error=SegmentsAIError.NetworkError,
                            error_message=str(e))

        except exceptions.TimeoutError as e:
            # raise exceptions.TimeoutError(
                # 'Request timed out while adding sample.'
            # ) from e
            return Response(ok=False, error=SegmentsAIError.TimeoutError,
                            error_message=str(e))
        except exceptions.UnexpectedError as e:
            # raise exceptions.UnexpectedError(
            #     'An unexpected error occurred while adding sample.'
            # ) from e
            return Response(ok=False, error=SegmentsAIError.UnexpectedError,
                            error_message=str(e))
