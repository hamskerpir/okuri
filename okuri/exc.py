from abc import ABCMeta


class InitializationError(Exception):
    """Exception raised for errors in the construction of okuri resources."""


class InternalResourceNotFoundError(Exception):
    """Exception raised when okuri can not find some backend resource."""


class StateMachineError(Exception, metaclass=ABCMeta):
    """Exception raised for errors in state machine execution."""


class NonDeterministicStateError(StateMachineError):
    """Exception raised when a state machine encounters a non-deterministic state."""


class SystemException(Exception, metaclass=ABCMeta):
    """Base class for exceptions that provide informative messages to the system."""
