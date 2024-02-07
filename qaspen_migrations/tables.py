from qaspen import BaseTable, fields


class QaspenMigrationTable(BaseTable):
    version = fields.VarCharField(max_length=32)
    created_at = fields.TimestampField()
