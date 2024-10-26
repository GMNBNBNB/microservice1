import pymysql
from .BaseDataService import DataDataService


class MySQLRDBDataService(DataDataService):
    """
    A generic data service for MySQL databases. The class implement common
    methods from BaseDataService and other methods for MySQL. More complex use cases
    can subclass, reuse methods and extend.
    """

    def __init__(self, context):
        super().__init__(context)

    def _get_connection(self):
        connection = pymysql.connect(
            host=self.context["host"],
            port=self.context["port"],
            user=self.context["user"],
            passwd=self.context["password"],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return connection

    def get_data_object(self,
                        database_name: str,
                        collection_name: str,
                        key_field: str,
                        key_value: str):
        """
        See base class for comments.
        """

        connection = None
        result = None

        try:
            sql_statement = f"SELECT * FROM {database_name}.{collection_name} " + \
                        f"where {key_field}=%s"
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, [key_value])
            result = cursor.fetchone()


        except Exception as e:
            if connection:
                connection.close()

        return result

    def get_all_data(self, database_name: str, collection_name: str, skip: int = 0, limit: int = 10) -> list[dict]:
        """
        Retrieve all data objects from the specified database and collection/table with pagination.
        :param skip: Number of records to skip.
        :param limit: Number of records to retrieve.
        :return: List of data objects.
        """
        connection = None
        results = []

        try:
            sql_statement = f"SELECT * FROM `{database_name}`.`{collection_name}` LIMIT %s OFFSET %s"
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, (limit, skip))
            results = cursor.fetchall()

        except Exception as e:
            print(f"Error in get_all_data: {e}")
            if connection:
                connection.close()

        return results

    def update_data(self,
                    database_name: str,
                    collection_name: str,
                    data: dict,
                    key_field: str,
                    key_value: str):
        """
        Update a data object in the specified database and collection/table.
        """

        connection = None

        try:
            set_clause = ", ".join([f"{field}=%s" for field in data.keys()])
            sql_statement = f"UPDATE {database_name}.{collection_name} SET {set_clause} WHERE {key_field}=%s"
            values = list(data.values()) + [key_value]

            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, values)
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()

        finally:
            if connection:
                connection.close()

    def delete_data(self,
                    database_name: str,
                    collection_name: str,
                    key_field: str,
                    key_value: str):
        """
        Delete a data object from the specified database and collection/table.
        """

        connection = None

        try:
            sql_statement = f"DELETE FROM {database_name}.{collection_name} WHERE {key_field}=%s"
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, [key_value])
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()

        finally:
            if connection:
                connection.close()

    def insert_data(self, database_name: str, collection_name: str, data: dict):
        """
        Inserts a new data object into the specified database and collection/table.
        """

        connection = None

        try:
            fields = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            sql_statement = f"INSERT INTO {database_name}.{collection_name} ({fields}) VALUES ({placeholders})"

            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, list(data.values()))
            connection.commit()

        except Exception as e:
            if connection:
                connection.rollback()
            print(f"Error occurred while inserting data: {e}")
            raise

        finally:
            if connection:
                connection.close()

