"""Module for defining preprocessing response and error types."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class PreprocessingError(Enum):
    """Define possible errors for labelling pre-processing."""

    SegmentsValidationError = 'SegmentsValidationError'
    SegmentsAPILimitError = 'SegmentsAPILimitError'
    SegmentsNotFoundError = 'SegmentsNotFoundError'
    SegmentsNetworkError = 'SegmentsNetworkError'
    SegmentsAlreadyExistsError = 'SegmentsAlreadyExistsError'
    SegmentsTimeoutError = 'SegmentsTimeoutError'
    SegmentsUnknownError = 'SegmentsUnknownError'


@dataclass
class PreprocessingResponse:
    """Define a response for labelling pre-processing actions."""

    ok: bool = None
    metadata: Any = None
    error: PreprocessingError = None
    error_message: str = None
