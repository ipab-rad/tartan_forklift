"""Segments.ai client wrapper module."""

from labelling_preproc.common.response import (
    PreprocessingError,
    PreprocessingResponse,
)

from segments import SegmentsClient, exceptions, typing


class SegmentsClientWrapper:
    """Class to interface with Segments.ai SDK."""

    def __init__(self, api_key: str):
        """
        Initialise the Segments client with an API key.

        Args:
            api_key: Segments.ai API key.
        """
        self.client = SegmentsClient(api_key)

    def verify_dataset(self, dataset_name: str) -> PreprocessingResponse:
        """
        Verify that a dataset exists on Segments.ai.

        Args:
            dataset_name: The name of the dataset to verify.
        Returns:
            PreprocessingResponse: Indicates success or failure and error info.
        """
        return self._handle_segments_errors(
            func=self.client.get_dataset, dataset_identifier=dataset_name
        )

    def add_sample(
        self, dataset_name: str, sequence_name: str, _attributes: dict
    ) -> PreprocessingResponse:
        """
        Add a sample to a Segments.ai dataset.

        Args:
            dataset_name: The name of the dataset.
            sequence_name: The sequence name within the dataset.
            attributes: A dictionary containing sample attributes.

        Returns:
            PreprocessingResponse: Indicates success or failure and error info.
        """
        return self._handle_segments_errors(
            func=self.client.add_sample,
            dataset_identifier=dataset_name,
            name=sequence_name,
            attributes=_attributes,
        )

    def add_dataset(
        self,
        dataset_name: str,
        task_type: str,
        dataset_attributes: dict,
        readme_str: str,
        organisation_name: str,
    ) -> PreprocessingResponse:
        """
        Add a new dataset in Segments.ai.

        For further reference see:
             https://sdkdocs.segments.ai/en/latest/client.html#create-a-dataset

        Args:
            dataset_name: Name of the dataset.
            task_type: The type of the dataset.
            dataset_attributes: A dictionary containing format and labels'
                                categories.
            readme_str: A string describing the summary of the dataset.
            organisation_name: Segments.ai organisation name
                               where the dataset is going to be created.
        Returns:
            PreprocessingResponse: Indicates success or failure and error info.
        """
        return self._handle_segments_errors(
            func=self.client.add_dataset,
            name=dataset_name,
            task_type=task_type,
            task_attributes=dataset_attributes,
            category=typing.Category.STREET_SCENERY,  # Automotive category
            readme=readme_str,
            enable_3d_cuboid_rotation=True,
            organization=organisation_name,
        )

    def _handle_segments_errors(
        self, func, *args, **kwargs
    ) -> PreprocessingResponse:
        """
        Handle errors generically for SegmentsClient operations.

        Args:
            context_str: A descriptive action string for context in
                         error messages.
            func (callable): The client method to call.
            *args, **kwargs: Arguments for the client method.

        Returns:
            PreprocessingResponse: Response indicating success or error.
        """
        try:
            func_result = func(*args, **kwargs)
            return PreprocessingResponse(ok=True, metadata=func_result)
        except exceptions.ValidationError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsValidationError,
                error_message=str(e),
            )
        except exceptions.APILimitError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsAPILimitError,
                error_message=str(e),
            )
        except exceptions.NotFoundError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsNotFoundError,
                error_message=str(e),
            )
        except exceptions.AlreadyExistsError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsAlreadyExistsError,
                error_message=str(e),
            )
        except exceptions.NetworkError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsNetworkError,
                error_message=str(e),
            )
        except exceptions.TimeoutError as e:
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsTimeoutError,
                error_message=str(e),
            )
        except Exception as e:  # noqa: B902
            return PreprocessingResponse(
                ok=False,
                error=PreprocessingError.SegmentsUnknownError,
                error_message=str(e),
            )
