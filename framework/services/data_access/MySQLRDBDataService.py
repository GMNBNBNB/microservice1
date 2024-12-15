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
            sql_statement = f"""SELECT r.recipe_id, r.name, r.steps, r.time_to_cook, r.meal_type, r.calories, r.rating,
                                i.ingredient_id, i.ingredient_name, i.quantity
                                FROM `{database_name}`.`{collection_name}` r
                                LEFT JOIN `{database_name}`.`ingredients` i ON r.recipe_id = i.recipe_id
                                WHERE r.{key_field}=%s"""

            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(sql_statement, [key_value])
            rows = cursor.fetchall()
            print(rows)

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
                            "ingredient_id": row["ingredient_id"],
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
                f"SELECT i.ingredient_id, i.recipe_id, i.ingredient_name, i.quantity "
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
                    "ingredient_id": ingredient["ingredient_id"],
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

    # def update_data(self,
    #                 database_name: str,
    #                 collection_name: str,
    #                 data: dict,
    #                 key_field: str,
    #                 key_value: any):
    #     """
    #     Update a data object in the specified database and collection/table,
    #     and update related ingredients if provided.
    #     """
    #     connection = None

    #     try:
    #         connection = self._get_connection()
    #         cursor = connection.cursor()

    #         connection.begin()

    #         ingredients = data.pop('ingredients', None)
    #         data.pop('links', None)
    #         data.pop('recipe_id', None)

    #         for key in list(data.keys()):
    #             if isinstance(data[key], (dict, list)):
    #                 print(f"Removing field '{key}' with non-serializable value: {data[key]}")
    #                 data.pop(key)

    #         if data:
    #             set_clause = ", ".join([f"`{field}`=%s" for field in data.keys()])
    #             sql_statement = f"UPDATE `{database_name}`.`{collection_name}` SET {set_clause} WHERE `{key_field}`=%s"
    #             values = list(data.values()) + [key_value]

    #             print("Data before SQL execution:", data)
    #             print("Values before SQL execution:", values)

    #             cursor.execute(sql_statement, values)
    #             print(f"Updated recipes table for {key_field}={key_value}")

    #         select_sql = f"SELECT recipe_id FROM `{database_name}`.`{collection_name}` WHERE `{key_field}`=%s"
    #         cursor.execute(select_sql, [key_value])
    #         result = cursor.fetchone()
    #         if not result:
    #             raise Exception(f"Recipe with {key_field}={key_value} not found")
    #         recipe_id = result['recipe_id']

    #         if ingredients is not None:
    #             delete_sql = f"DELETE FROM `{database_name}`.`ingredients` WHERE `recipe_id`=%s"
    #             cursor.execute(delete_sql, [recipe_id])
    #             print(f"Deleted existing ingredients for recipe_id={recipe_id}")

    #             insert_sql = (
    #                 f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
    #                 f"VALUES (%s, %s, %s)"
    #             )
    #             ingredient_values = [
    #                 (recipe_id, ingredient['ingredient_name'], ingredient['quantity'])
    #                 for ingredient in ingredients
    #             ]
    #             cursor.executemany(insert_sql, ingredient_values)
    #             print(f"Inserted {len(ingredients)} new ingredients for recipe_id={recipe_id}")

    #         connection.commit()
    #         print("Transaction committed successfully.")

    #     except Exception as e:
    #         print(f"Error in update_data: {e}")
    #         if connection:
    #             connection.rollback()
    #             print("Transaction rolled back due to error.")
    #         raise

    #     finally:
    #         if connection:
    #             connection.close()
    #             print("Database connection closed.")

    def update_data(self,
                database_name: str,
                collection_name: str,
                data: dict,
                key_field: str,
                key_value: any):
        """
        Update a data object in the specified database and collection/table,
        and properly update related ingredients if provided.
        """
        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            connection.begin()

            # Extract ingredients and remove unneeded fields
            ingredients = data.pop('ingredients', None)
            data.pop('links', None)
            data.pop('recipe_id', None)

            # Remove fields with non-serializable values
            for key in list(data.keys()):
                if isinstance(data[key], (dict, list)):
                    print(f"Removing field '{key}' with non-serializable value: {data[key]}")
                    data.pop(key)

            # Update the main recipe data
            if data:
                set_clause = ", ".join([f"`{field}`=%s" for field in data.keys()])
                sql_statement = f"UPDATE `{database_name}`.`{collection_name}` SET {set_clause} WHERE `{key_field}`=%s"
                values = list(data.values()) + [key_value]

                print("Data before SQL execution:", data)
                print("Values before SQL execution:", values)

                cursor.execute(sql_statement, values)
                print(f"Updated recipes table for {key_field}={key_value}")

            # Get the recipe ID
            select_sql = f"SELECT recipe_id FROM `{database_name}`.`{collection_name}` WHERE `{key_field}`=%s"
            cursor.execute(select_sql, [key_value])
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Recipe with {key_field}={key_value} not found")
            recipe_id = result['recipe_id']

            # Update ingredients if provided
            if ingredients is not None:
                # Get existing ingredients for the recipe
                select_ingredients_sql = (
                    f"SELECT `ingredient_name`, `quantity` FROM `{database_name}`.`ingredients` WHERE `recipe_id`=%s"
                )
                cursor.execute(select_ingredients_sql, [recipe_id])
                existing_ingredients = {row['ingredient_name']: row['quantity'] for row in cursor.fetchall()}

                # Separate ingredients into update, insert, and delete groups
                ingredients_to_update = []
                ingredients_to_insert = []
                existing_names = set(existing_ingredients.keys())
                provided_names = set(ingredient['ingredient_name'] for ingredient in ingredients)

                # Determine which ingredients to update or insert
                for ingredient in ingredients:
                    name = ingredient['ingredient_name']
                    quantity = ingredient['quantity']
                    if name in existing_ingredients:
                        if existing_ingredients[name] != quantity:
                            ingredients_to_update.append((quantity, recipe_id, name))
                    else:
                        ingredients_to_insert.append((recipe_id, name, quantity))

                # Determine which ingredients to delete
                ingredients_to_delete = list(existing_names - provided_names)

                # Perform updates
                if ingredients_to_update:
                    update_sql = (
                        f"UPDATE `{database_name}`.`ingredients` "
                        f"SET `quantity`=%s WHERE `recipe_id`=%s AND `ingredient_name`=%s"
                    )
                    cursor.executemany(update_sql, ingredients_to_update)
                    print(f"Updated {len(ingredients_to_update)} ingredients.")

                # Perform inserts
                if ingredients_to_insert:
                    insert_sql = (
                        f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
                        f"VALUES (%s, %s, %s)"
                    )
                    cursor.executemany(insert_sql, ingredients_to_insert)
                    print(f"Inserted {len(ingredients_to_insert)} new ingredients.")

                # Perform deletions
                if ingredients_to_delete:
                    delete_sql = (
                        f"DELETE FROM `{database_name}`.`ingredients` "
                        f"WHERE `recipe_id`=%s AND `ingredient_name`=%s"
                    )
                    cursor.executemany(delete_sql, [(recipe_id, name) for name in ingredients_to_delete])
                    print(f"Deleted {len(ingredients_to_delete)} ingredients.")

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
            # delete_nutrition_sql = f"DELETE FROM `nutrition_db`.`nutrition` WHERE `recipe_id`=%s"
            # cursor.execute(delete_nutrition_sql, [recipe_id])
            # print(f"Deleted nutrition information for recipe_id={recipe_id}")

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

    # def insert_data(self, database_name: str, collection_name: str, data: dict):
    #     """
    #     Insert a new recipe into the database, including its ingredients.

    #     :param database_name: Name of the database.
    #     :param collection_name: Name of the recipes table.
    #     :param data: Dictionary containing recipe data, including 'ingredients'.
    #     :return: The inserted recipe data, including the generated 'recipe_id'.
    #     """
    #     connection = None

    #     try:
    #         connection = self._get_connection()
    #         cursor = connection.cursor()

    #         connection.begin()

    #         data.pop('links', None)
    #         data.pop('recipe_id', None)
    #         ingredients = data.pop('ingredients', [])


    #         for key in list(data.keys()):
    #             if isinstance(data[key], (dict, list)):
    #                 print(f"Removing field '{key}' with non-serializable value: {data[key]}")
    #                 data.pop(key)


    #         recipe_fields = list(data.keys())
    #         recipe_values = list(data.values())

    #         fields = ', '.join([f"`{field}`" for field in recipe_fields])
    #         placeholders = ', '.join(['%s'] * len(recipe_fields))
    #         insert_recipe_sql = f"INSERT INTO `{database_name}`.`{collection_name}` ({fields}) VALUES ({placeholders})"

    #         cursor.execute(insert_recipe_sql, recipe_values)
    #         recipe_id = cursor.lastrowid
    #         print(f"Inserted recipe '{data.get('name')}' with ID {recipe_id} into '{collection_name}' table.")


    #         if ingredients:
    #             insert_ingredient_sql = (
    #                 f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
    #                 f"VALUES (%s, %s, %s)"
    #             )
    #             ingredient_values = [
    #                 (recipe_id, ingredient['ingredient_name'], ingredient['quantity'])
    #                 for ingredient in ingredients
    #             ]
    #             cursor.executemany(insert_ingredient_sql, ingredient_values)
    #             print(f"Inserted {len(ingredients)} ingredients for recipe '{data.get('name')}'.")


    #         connection.commit()
    #         print("Transaction committed successfully.")


    #         data['recipe_id'] = recipe_id
    #         data['ingredients'] = ingredients
    #         return data

    #     except pymysql.err.IntegrityError as e:
    #         print(f"Integrity error in insert_data: {e}")
    #         if connection:
    #             connection.rollback()
    #             print("Transaction rolled back due to integrity error.")
    #         raise e
    #     except Exception as e:
    #         print(f"Error in insert_data: {e}")
    #         if connection:
    #             connection.rollback()
    #             print("Transaction rolled back due to error.")
    #         raise e
    #     finally:
    #         if connection:
    #             connection.close()
    #             print("Database connection closed.")
    def insert_data(self, database_name: str, collection_name: str, data: dict):
        """
        Insert a new recipe into the database, including its ingredients.

        :param database_name: Name of the database.
        :param collection_name: Name of the recipes table.
        :param data: Dictionary containing recipe data, including 'ingredients'.
        :return: The inserted recipe data, including the generated 'recipe_id' and 'ingredient_id' for each ingredient.
        """
        connection = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            connection.begin()

            # Remove unwanted fields from data
            data.pop('links', None)
            data.pop('recipe_id', None)
            ingredients = data.pop('ingredients', [])

            # Remove non-serializable fields
            for key in list(data.keys()):
                if isinstance(data[key], (dict, list)):
                    print(f"Removing field '{key}' with non-serializable value: {data[key]}")
                    data.pop(key)

            # Insert recipe
            recipe_fields = list(data.keys())
            recipe_values = list(data.values())
            fields = ', '.join([f"`{field}`" for field in recipe_fields])
            placeholders = ', '.join(['%s'] * len(recipe_fields))
            insert_recipe_sql = f"INSERT INTO `{database_name}`.`{collection_name}` ({fields}) VALUES ({placeholders})"

            cursor.execute(insert_recipe_sql, recipe_values)
            recipe_id = cursor.lastrowid
            print(f"Inserted recipe '{data.get('name')}' with ID {recipe_id} into '{collection_name}' table.")

            ingredient_ids = []
            if ingredients:
                # Insert ingredients and fetch their IDs
                insert_ingredient_sql = (
                    f"INSERT INTO `{database_name}`.`ingredients` (`recipe_id`, `ingredient_name`, `quantity`) "
                    f"VALUES (%s, %s, %s)"
                )
                for ingredient in ingredients:
                    cursor.execute(insert_ingredient_sql, (recipe_id, ingredient['ingredient_name'], ingredient['quantity']))
                    ingredient_id = cursor.lastrowid
                    ingredient['ingredient_id'] = ingredient_id  # Add generated ID to ingredient
                    ingredient_ids.append(ingredient_id)
                    print(f"Inserted ingredient '{ingredient['ingredient_name']}' with ID {ingredient_id}.")

            connection.commit()
            print("Transaction committed successfully.")

            # Return recipe data including generated IDs
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




