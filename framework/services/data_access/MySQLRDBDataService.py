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
                        key_value: any):

        connection = None
        result = None

        try:
            # 调整 SQL 语句，使用参数化查询
            sql_statement = f"""SELECT r.recipe_id, r.name, r.steps, r.time_to_cook, r.meal_type, r.calories, r.rating,
                                i.ingredient_name, i.quantity
                                FROM `{database_name}`.`{collection_name}` r
                                LEFT JOIN `{database_name}`.`ingredients` i ON r.recipe_id = i.recipe_id
                                WHERE r.{key_field}=%s"""

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

        finally:
            if connection:
                connection.close()

        return result

    def get_all_data(self, database_name: str, collection_name: str, skip: int = 0, limit: int = 10) -> list[dict]:
        """
        Retrieve all data objects from the specified database and collection/table with pagination,
        including related ingredients.
        """
        connection = None
        results = []

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            recipes_sql = (
                f"SELECT r.recipe_id, r.name, r.steps, r.time_to_cook, r.meal_type, "
                f"r.calories, r.rating "
                f"FROM `{database_name}`.`{collection_name}` r "
                f"LIMIT %s OFFSET %s"
            )
            cursor.execute(recipes_sql, (limit, skip))
            recipes = cursor.fetchall()

            if not recipes:
                return []

            recipe_ids = [recipe["recipe_id"] for recipe in recipes]

            format_strings = ','.join(['%s'] * len(recipe_ids))
            ingredients_sql = (
                f"SELECT i.recipe_id, i.ingredient_name, i.quantity "
                f"FROM `{database_name}`.ingredients i "
                f"WHERE i.recipe_id IN ({format_strings})"
            )
            cursor.execute(ingredients_sql, recipe_ids)
            ingredients = cursor.fetchall()

            ingredients_map = {}
            for ingredient in ingredients:
                recipe_id = ingredient["recipe_id"]
                if recipe_id not in ingredients_map:
                    ingredients_map[recipe_id] = []
                ingredients_map[recipe_id].append({
                    "ingredient_name": ingredient["ingredient_name"],
                    "quantity": ingredient["quantity"]
                })

            for recipe in recipes:
                recipe_dict = {
                    "recipe_id": recipe["recipe_id"],
                    "name": recipe["name"],
                    "steps": recipe["steps"],
                    "time_to_cook": recipe["time_to_cook"],
                    "meal_type": recipe["meal_type"],
                    "calories": recipe["calories"],
                    "rating": recipe["rating"],
                    "ingredients": ingredients_map.get(recipe["recipe_id"], [])
                }
                results.append(recipe_dict)

        except Exception as e:
            print(f"Error in get_all_data: {e}")
            if connection:
                connection.rollback()
        finally:
            if connection:
                connection.close()

        return results

    def update_data(self,
                    database_name: str,
                    collection_name: str,
                    data: dict,
                    key_field: str,
                    key_value: any):
        """
        Update a data object in the specified database and collection/table,
        and update related ingredients if provided.
        """
        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            connection.begin()

            ingredients = data.pop('ingredients', None)
            data.pop('_links', None)
            data.pop('recipe_id', None)

            for key in list(data.keys()):
                if isinstance(data[key], (dict, list)):
                    print(f"Removing field '{key}' with non-serializable value: {data[key]}")
                    data.pop(key)

            if data:
                set_clause = ", ".join([f"`{field}`=%s" for field in data.keys()])
                sql_statement = f"UPDATE `{database_name}`.`{collection_name}` SET {set_clause} WHERE `{key_field}`=%s"
                values = list(data.values()) + [key_value]

                print("Data before SQL execution:", data)
                print("Values before SQL execution:", values)

                cursor.execute(sql_statement, values)
                print(f"Updated recipes table for {key_field}={key_value}")

            select_sql = f"SELECT recipe_id FROM `{database_name}`.`{collection_name}` WHERE `{key_field}`=%s"
            cursor.execute(select_sql, [key_value])
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Recipe with {key_field}={key_value} not found")
            recipe_id = result['recipe_id']

            if ingredients is not None:
                delete_sql = f"DELETE FROM `{database_name}`.`ingredients` WHERE `recipe_id`=%s"
                cursor.execute(delete_sql, [recipe_id])
                print(f"Deleted existing ingredients for recipe_id={recipe_id}")

                insert_sql = (
                    f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
                    f"VALUES (%s, %s, %s)"
                )
                ingredient_values = [
                    (recipe_id, ingredient['ingredient_name'], ingredient['quantity'])
                    for ingredient in ingredients
                ]
                cursor.executemany(insert_sql, ingredient_values)
                print(f"Inserted {len(ingredients)} new ingredients for recipe_id={recipe_id}")

            connection.commit()
            print("Transaction committed successfully.")

        except Exception as e:
            print(f"Error in update_data: {e}")
            if connection:
                connection.rollback()
                print("Transaction rolled back due to error.")
            raise

        finally:
            if connection:
                connection.close()
                print("Database connection closed.")

    def delete_data(self,
                    database_name: str,
                    collection_name: str,
                    key_field: str,
                    key_value: any):
        """
        Delete a data object from the specified database and collection/table,
        including related ingredients and nutrition information.
        """

        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # Start transaction
            connection.begin()

            # Get recipe_id if key_field is not 'recipe_id'
            if key_field != 'recipe_id':
                select_sql = f"SELECT recipe_id FROM `{database_name}`.`{collection_name}` WHERE `{key_field}`=%s"
                cursor.execute(select_sql, [key_value])
                result = cursor.fetchone()
                if not result:
                    raise Exception(f"Recipe with {key_field}={key_value} not found")
                recipe_id = result['recipe_id']
            else:
                recipe_id = key_value

            # Delete related records from 'nutrition' table
            delete_nutrition_sql = f"DELETE FROM `nutrition_db`.`nutrition` WHERE `recipe_id`=%s"
            cursor.execute(delete_nutrition_sql, [recipe_id])
            print(f"Deleted nutrition information for recipe_id={recipe_id}")

            # Delete related records from 'ingredients' table
            delete_ingredients_sql = f"DELETE FROM `{database_name}`.`ingredients` WHERE `recipe_id`=%s"
            cursor.execute(delete_ingredients_sql, [recipe_id])
            print(f"Deleted ingredients for recipe_id={recipe_id}")

            # Delete recipe from 'recipes' table
            delete_recipe_sql = f"DELETE FROM `{database_name}`.`{collection_name}` WHERE `recipe_id`=%s"
            cursor.execute(delete_recipe_sql, [recipe_id])
            print(f"Deleted recipe with recipe_id={recipe_id}")

            # Commit transaction
            connection.commit()
            print("Transaction committed successfully.")

        except Exception as e:
            print(f"Error in delete_data: {e}")
            if connection:
                connection.rollback()
                print("Transaction rolled back due to error.")
            raise e
        finally:
            if connection:
                connection.close()
                print("Database connection closed.")

    def insert_data(self, database_name: str, collection_name: str, data: dict):
        """
        Insert a new recipe into the database, including its ingredients.

        :param database_name: Name of the database.
        :param collection_name: Name of the recipes table.
        :param data: Dictionary containing recipe data, including 'ingredients'.
        :return: The inserted recipe data, including the generated 'recipe_id'.
        """
        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            connection.begin()

            data.pop('links', None)
            data.pop('recipe_id', None)
            ingredients = data.pop('ingredients', [])


            for key in list(data.keys()):
                if isinstance(data[key], (dict, list)):
                    print(f"Removing field '{key}' with non-serializable value: {data[key]}")
                    data.pop(key)


            recipe_fields = list(data.keys())
            recipe_values = list(data.values())

            fields = ', '.join([f"`{field}`" for field in recipe_fields])
            placeholders = ', '.join(['%s'] * len(recipe_fields))
            insert_recipe_sql = f"INSERT INTO `{database_name}`.`{collection_name}` ({fields}) VALUES ({placeholders})"

            cursor.execute(insert_recipe_sql, recipe_values)
            recipe_id = cursor.lastrowid
            print(f"Inserted recipe '{data.get('name')}' with ID {recipe_id} into '{collection_name}' table.")


            if ingredients:
                insert_ingredient_sql = (
                    f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
                    f"VALUES (%s, %s, %s)"
                )
                ingredient_values = [
                    (recipe_id, ingredient['ingredient_name'], ingredient['quantity'])
                    for ingredient in ingredients
                ]
                cursor.executemany(insert_ingredient_sql, ingredient_values)
                print(f"Inserted {len(ingredients)} ingredients for recipe '{data.get('name')}'.")


            connection.commit()
            print("Transaction committed successfully.")


            data['recipe_id'] = recipe_id
            data['ingredients'] = ingredients
            return data

        except pymysql.err.IntegrityError as e:
            print(f"Integrity error in insert_data: {e}")
            if connection:
                connection.rollback()
                print("Transaction rolled back due to integrity error.")
            raise e
        except Exception as e:
            print(f"Error in insert_data: {e}")
            if connection:
                connection.rollback()
                print("Transaction rolled back due to error.")
            raise e
        finally:
            if connection:
                connection.close()
                print("Database connection closed.")



