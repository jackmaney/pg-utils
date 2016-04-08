import os
import psycopg2

__all__ = ["Connection"]


class Connection(object):
    """
    This is a wrapper class around psycopg2's ``Connection`` object.
    Its main purpose is for the simple specification of
    login information via environment variables.

    :param str username: Username. Overrides the corresponding environment variable.
    :param str password: Password. Overrides the corresponding environment variable.
    :param str hostname: Hostname. Overrides the corresponding environment variable.
    :param str database: The name of the database. Overrides the corresponding environment variable.
    :param str env_username: The name of the environment variable to use for your username.
    :param str env_password: The name of the environment variable to use for your password.
    :param str env_hostname: The name of the environment variable to use for the hostname.
    :param str env_database: The name of the environment variable to use for the database.
    :param None|dict other_connection_kwargs: Other keyword arguments (if any) that you'd like to pass to the psycopg2 ``Connection`` object.

    :ivar psycopg2.extensions.connection connection: The resulting raw connection object.

    """
    def __init__(self, username=None, password=None,
                 hostname=None, database=None,
                 env_username="pg_username",
                 env_password="pg_password",
                 env_hostname="pg_hostname",
                 env_database="pg_database",
                 **other_connection_kwargs):

        self.username = username or os.getenv(env_username)
        self.password = password or os.getenv(env_password)
        self.hostname = hostname or os.getenv(env_hostname)
        self.database = database or os.getenv(env_database)

        other_connection_kwargs = other_connection_kwargs or {}

        if not self.username or not self.password \
                or not self.hostname or not self.database:

            raise ValueError("Unable to retrieve username, password, host, and database ({},{},{},{})".format(
                self.username, self.password, self.hostname, self.database))

        self.connection = psycopg2.connect(
            database=self.database, user=self.username,
            password=self.password, host=self.hostname,
            **other_connection_kwargs)

    def close(self):
        """
        A simple wrapper around the ``close`` method of the ``connection`` attribute.
        """
        self.connection.close()

    def rollback(self):
        """
        A simple wrapper around the ``rollback`` method of the ``connection`` attribute.
        """
        self.connection.rollback()

    def commit(self):
        """
        A simple wrapper around the ``commit`` method of the ``connection`` attribute.
        """
        self.connection.commit()

    def cursor(self, *args, **kwargs):
        """
        A simple wrapper around the ``cursor`` factory method of the ``connection`` attribute.
        """
        return self.connection.cursor(*args, **kwargs)

    def __del__(self):
        """
        The raw connection is closed upon garbage collection of this object.
        """
        self.close()

        del self
