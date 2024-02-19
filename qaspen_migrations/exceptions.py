class QaspenMigrationsError(Exception):
    """Base class for all exceptions."""


class ConfigurationError(QaspenMigrationsError):
    """Raises when configuration is invalid."""


class ColumnParsingError(QaspenMigrationsError):
    """Raises when column or column cannot be parsed."""


class MigrationGenerationError(QaspenMigrationsError):
    """Raises when migration generation proccess cannot be completed."""
