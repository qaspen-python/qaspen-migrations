class QaspenMigrationsError(Exception):
    """Base class for all exceptions."""


class ConfigurationError(QaspenMigrationsError):
    """Raises when configuration is invalid."""


class FieldParsingError(QaspenMigrationsError):
    """Raises when field or column cannot be parsed."""


class InconsistentTableError(QaspenMigrationsError):
    """Raises when local table doesn't match table from database."""
