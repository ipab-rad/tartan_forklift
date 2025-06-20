from dataclasses import dataclass
from enum import Enum


class SegmentsAIError(Enum):
    """
    Define possible errors when working with SegmentsAi client.
    """
    ValidationError = 'ValidationError'
    APILimitError = 'APILimitError'
    NotFoundError = 'NotFoundError'
    NetworkError = 'NetworkError'
    AlreadyExistsError = 'AlreadyExistsError'
    TimeoutError = 'TimeoutError'
    UnexpectedError = 'UnexpectedError'


@dataclass
class Response():
    """
    Define a status of a certain action.
    """
    ok: bool = None
    error: SegmentsAIError = None
    error_message: str = None


class State(Enum):
    """
    Define possible states of the SegmentsAi client.
    """
    INITIALISED = 'INITIALISED'
    READY = 'READY'
    UPLOADING = 'UPLOADING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
