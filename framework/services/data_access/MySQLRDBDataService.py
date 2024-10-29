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

    def get_total_count(self, database_name: str, collection_name: str) -> int:
        """
        获取指定数据库和集合/表的总记录数。
        """
        connection = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            sql = f"SELECT COUNT(*) as count FROM `{database_name}`.`{collection_name}`"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                return result["count"]
            return 0
        except Exception as e:
            print(f"Error in get_total_count: {e}")
            raise e
        finally:
            if connection:
                connection.close()

    def get_data_object(self,
                        database_name: str,
                        collection_name: str,
                        key_field: str,
                        key_value: str):

        connection = None
        result = None

        try:
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
        Retrieve all data objects from the specified database and collection/table with pagination,
        including related ingredients.

        :param database_name: Name of the database.
        :param collection_name: Name of the table/collection.
        :param skip: Number of records to skip for pagination.
        :param limit: Number of records to retrieve for pagination.
        :return: List of data objects with their related ingredients.
        """
        connection = None
        results = []

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # 1. 首先获取分页后的食谱列表
            recipes_sql = (
                f"SELECT r.recipe_id, r.name, r.steps, r.time_to_cook, r.meal_type, "
                f"r.calories, r.rating, r.kid_friendly "
                f"FROM `{database_name}`.`{collection_name}` r "
                f"LIMIT %s OFFSET %s"
            )
            cursor.execute(recipes_sql, (limit, skip))
            recipes = cursor.fetchall()

            if not recipes:
                return []

            # 2. 获取所有选中食谱的名称
            recipe_names = [recipe["name"] for recipe in recipes]

            # 3. 根据食谱名称获取所有相关的食材
            format_strings = ','.join(['%s'] * len(recipe_names))
            ingredients_sql = (
                f"SELECT i.recipe_name, i.ingredient_name, i.quantity "
                f"FROM `{database_name}`.ingredients i "
                f"WHERE i.recipe_name IN ({format_strings})"
            )
            cursor.execute(ingredients_sql, recipe_names)
            ingredients = cursor.fetchall()

            # 4. 将食材按食谱名称分组
            ingredients_map = {}
            for ingredient in ingredients:
                recipe_name = ingredient["recipe_name"]
                if recipe_name not in ingredients_map:
                    ingredients_map[recipe_name] = []
                ingredients_map[recipe_name].append({
                    "ingredient_name": ingredient["ingredient_name"],
                    "quantity": ingredient["quantity"]
                })

            # 5. 将食材聚合到对应的食谱中
            for recipe in recipes:
                recipe_dict = {
                    "recipe_id": recipe["recipe_id"],
                    "name": recipe["name"],
                    "steps": recipe["steps"],
                    "time_to_cook": recipe["time_to_cook"],
                    "meal_type": recipe["meal_type"],
                    "calories": recipe["calories"],
                    "rating": recipe["rating"],
                    "kid_friendly": recipe["kid_friendly"],
                    "ingredients": ingredients_map.get(recipe["name"], [])
                }
                results.append(recipe_dict)

        except Exception as e:
            print(f"Error in get_all_data: {e}")
            if connection:
                connection.rollback()  # 如果有必要，可以回滚事务
        finally:
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
        Update a data object in the specified database and collection/table,
        and update related ingredients if provided.

        :param database_name: Name of the database.
        :param collection_name: Name of the table/collection.
        :param data: Dictionary containing fields to update. If includes 'ingredients', update ingredients as well.
        :param key_field: Field name to identify the record (e.g., 'name').
        :param key_value: Value of the key_field to identify the record.
        """
        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # 开始事务
            connection.begin()

            # 提取食材数据（如果存在）
            ingredients = data.pop('ingredients', None)

            # 更新主表（recipes）
            if data:
                set_clause = ", ".join([f"`{field}`=%s" for field in data.keys()])
                sql_statement = f"UPDATE `{database_name}`.`{collection_name}` SET {set_clause} WHERE `{key_field}`=%s"
                values = list(data.values()) + [key_value]
                cursor.execute(sql_statement, values)
                print(f"Updated recipes table for {key_field}={key_value}")

            # 更新相关表（ingredients）
            if ingredients is not None:
                # 删除现有的食材记录
                delete_sql = f"DELETE FROM `{database_name}`.`ingredients` WHERE `recipe_name`=%s"
                cursor.execute(delete_sql, [key_value])
                print(f"Deleted existing ingredients for recipe_name={key_value}")

                # 插入新的食材记录
                insert_sql = (
                    f"INSERT INTO `{database_name}`.`ingredients` (`recipe_name`, `ingredient_name`, `quantity`) "
                    f"VALUES (%s, %s, %s)"
                )
                ingredient_values = [
                    (key_value, ingredient['ingredient_name'], ingredient['quantity'])
                    for ingredient in ingredients
                ]
                cursor.executemany(insert_sql, ingredient_values)
                print(f"Inserted {len(ingredients)} new ingredients for recipe_name={key_value}")

            # 提交事务
            connection.commit()
            print("Transaction committed successfully.")

        except Exception as e:
            print(f"Error in update_data: {e}")
            if connection:
                connection.rollback()
                print("Transaction rolled back due to error.")
            raise  # 重新抛出异常，以便上层处理

        finally:
            if connection:
                connection.close()
                print("Database connection closed.")

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

    # app/services/data_access/MySQLRDBDataService.py

    def insert_data(self, database_name: str, collection_name: str, data: dict):
        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # 开始事务
            connection.begin()

            # 1. 插入到 recipes 表，包括 recipe_id
            recipe_fields = ['recipe_id', 'name', 'steps', 'time_to_cook', 'meal_type', 'calories', 'rating',
                             'kid_friendly']
            recipe_values = [data.get(field) for field in recipe_fields]

            fields = ', '.join([f"`{field}`" for field in recipe_fields])
            placeholders = ', '.join(['%s'] * len(recipe_fields))
            insert_recipe_sql = f"INSERT INTO `{database_name}`.`{collection_name}` ({fields}) VALUES ({placeholders})"

            cursor.execute(insert_recipe_sql, recipe_values)
            print(
                f"Inserted recipe '{data.get('name')}' with ID {data.get('recipe_id')} into '{collection_name}' table.")

            # 2. 插入到 ingredients 表，使用提供的 recipe_id
            ingredients = data.get('ingredients', [])
            if ingredients:
                insert_ingredient_sql = (
                    f"INSERT INTO `{database_name}`.`ingredients` (`recipe_name`, `ingredient_name`, `quantity`) "
                    f"VALUES (%s, %s, %s)"
                )
                ingredient_values = [
                    (data.get('name'), ingredient['ingredient_name'], ingredient['quantity'])
                    for ingredient in ingredients
                ]
                cursor.executemany(insert_ingredient_sql, ingredient_values)
                print(f"Inserted {len(ingredients)} ingredients for recipe '{data.get('name')}'.")

            # 提交事务
            connection.commit()
            print("Transaction committed successfully.")

            # 返回新插入的食谱数据
            return data

        except pymysql.err.IntegrityError as e:
            print(f"Integrity error in insert_data: {e}")
            if connection:
                connection.rollback()
            raise e
        except Exception as e:
            print(f"Error in insert_data: {e}")
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                connection.close()
                print("Database connection closed.")


