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

        connection = None
        result = None

        try:
            # 构建 SQL 查询语句
            sql_statement = f"SELECT r.recipe_id, r.name, r.steps, r.time_to_cook, r.meal_type, r.calories, r.rating, r.kid_friendly," \
                            f"i.ingredient_name, i.quantity " \
                            f"FROM {database_name}.{collection_name} r " \
                            f"LEFT JOIN {database_name}.ingredients i ON r.name = i.recipe_name " \
                            f"WHERE {key_field}=%s"

            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, [key_value])
            rows = cursor.fetchall()

            if rows:
                recipe = {
                    "recipe_id": rows[0]["recipe_id"],
                    "name": rows[0]["name"],
                    "steps": rows[0]["steps"],
                    "time_to_cook": rows[0]["time_to_cook"],
                    "meal_type": rows[0]["meal_type"],
                    "calories": rows[0]["calories"],
                    "rating": rows[0]["rating"],
                    "kid_friendly": rows[0]["kid_friendly"],
                    "ingredients": []
                }

                for row in rows:
                    if row["ingredient_name"] is not None:
                        recipe["ingredients"].append({
                            "ingredient_name": row["ingredient_name"],
                            "quantity": row["quantity"]
                        })

                result = recipe

        except Exception as e:
            print(f"An error occurred: {e}")
            if connection:
                connection.close()
            raise

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

