import os
import psycopg2

__all__ = ["Connection"]

class Connection(object):
    def __init__(self, username=None, password=None,
                 hostname=None, database=None, env_username=None,
                 env_password=None, env_host=None, env_database=None):

        self.username = username or os.getenv(env_username)
        self.password = password or os.getenv(env_password)
        self.hostname = hostname or os.getenv(env_host)
        self.database = database or os.getenv(env_database)

        if not self.username or not self.password or not self.hostname or not self.database:
            raise ValueError("Unable to retrieve username, password, host, and database ({},{},{},{})".format(
                self.username, self.password, self.hostname, self.database))

        self.connection = psycopg2.connect(
            database=self.database, user=self.username,
            password=self.password, host=self.hostname)

    def close(self):
        self.connection.close()

    def rollback(self):
        self.connection.rollback()

    def commit(self):
        self.connection.commit()

    def cursor(self, *args, **kwargs):

        return self.connection.cursor(*args, **kwargs)

    def __del__(self):

        self.close()

        del self

