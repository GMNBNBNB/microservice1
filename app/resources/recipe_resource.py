from typing import Any
from typing import List
from framework.resources.base_resource import BaseResource

from app.models.recipe import Recipe
from app.services.service_factory import ServiceFactory


class RecipeResource(BaseResource):

    def __init__(self, config):
        super().__init__(config)

        # TODO -- Replace with dependency injection.
        #
        self.data_service = ServiceFactory.get_service("RecipeResourceDataService")
        self.database = "recipes_database"
        self.recipes = "recipes"
        self.key_field="name"

    def create_by_key(self, data: dict) -> Recipe:
        d_service = self.data_service
        d_service.insert_data(
            self.database, self.recipes, data
        )
        return Recipe(**data)

    def get_by_key(self, key: str) -> Recipe:
        d_service = self.data_service
        result = d_service.get_data_object(
            self.database, self.recipes, key_field=self.key_field, key_value=key
        )
        result = Recipe(**result)
        return result

    def get_all(self, skip: int = 0, limit: int = 10) -> List[Recipe]:
        """
        Retrieve all recipes from the database with pagination.
        :param skip: Number of records to skip.
        :param limit: Number of records to retrieve.
        :return: List of Recipe objects.
        """
        results = self.data_service.get_all_data(
            self.database, self.recipes, skip=skip, limit=limit
        )
        return [Recipe(**item) for item in results]

    def update_by_key(self, key: str, data: dict) -> Recipe:
        d_service = self.data_service
        d_service.update_data(
            self.database, self.recipes, data, key_field=self.key_field, key_value=key
        )
        return self.get_by_key(key)

    def delete_by_key(self, key: str) -> None:
        d_service = self.data_service
        d_service.delete_data(
            self.database, self.recipes, key_field=self.key_field, key_value=key
        )


