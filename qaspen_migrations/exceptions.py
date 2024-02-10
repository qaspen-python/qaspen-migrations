class QaspenMigrationsError(Exception):
    """Base class for all exceptions."""


class ConfigurationError(QaspenMigrationsError):
    """Raises when configuration is invalid."""


class FieldParsingError(QaspenMigrationsError):
    """Raises when field or column cannot be parsed."""


class MigrationGenerationError(QaspenMigrationsError):
    """Raises when migration generation proccess cannot be completed."""
