class QaspenMigrationError(Exception):
    """Base class for all exceptions."""


class ConfigurationError(QaspenMigrationError):
    """Raises when configuration is invalid."""


class ColumnParsingError(QaspenMigrationError):
    """Raises when column or column cannot be parsed."""


class MigrationGenerationError(QaspenMigrationError):
    """Raises when migration generation proccess cannot be completed."""


class MigrationCorruptionError(QaspenMigrationError):
    """Raises when liskov substitution principle is violated on a migration."""


class MigrationVersionError(QaspenMigrationError):
    """Raises when current migration version is inconsistent."""
