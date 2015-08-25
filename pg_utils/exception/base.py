class TableDoesNotExistError(Exception):
    """
    Thrown when creation of a ``Table`` object is attempted for a
    corresponding table that does not exist in the database.
    """
