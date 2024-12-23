import pytest
from pydantic import ValidationError
from typing import Any

from app.models.recipe import Ingredient, Recipe, PaginatedResponse

# Sample data for testing
valid_ingredient_data = {
    "ingredient_id": 1,
    "ingredient_name": "Avocado",
    "quantity": "1 large"
}

valid_recipe_data = {
    "recipe_id": 171,
    "name": "Avocado Toast",
    "ingredients": [
        {
            "ingredient_id": 1,
            "ingredient_name": "Avocado",
            "quantity": "1 large"
        },
        {
            "ingredient_id": 2,
            "ingredient_name": "Bread",
            "quantity": "2 slices"
        },
        {
            "ingredient_id": 3,
            "ingredient_name": "Lime",
            "quantity": "1/2"
        },
        {
            "ingredient_id": 4,
            "ingredient_name": "Olive oil",
            "quantity": "1 tbsp"
        },
        {
            "ingredient_id": 5,
            "ingredient_name": "Salt",
            "quantity": "1/4 tsp"
        }
    ],
    "steps": "1. Toast bread. 2. Mash avocado with lime. 3. Spread on toast and season.",
    "time_to_cook": 10,
    "meal_type": "breakfast",
    "calories": 300,
    "rating": 4.4,
    "links": {
        "self": {
            "href": "/recipes/171"
        },
        "update": {
            "href": "/recipes/171",
            "method": "PUT"
        },
        "delete": {
            "href": "/recipes/171",
            "method": "DELETE"
        }
    }
}

invalid_recipe_data_missing_name = {
    "recipe_id": 172,
    "ingredients": [
        {
            "ingredient_id": 6,
            "ingredient_name": "Tomato",
            "quantity": "2"
        }
    ]
}

invalid_recipe_data_wrong_type = {
    "recipe_id": "not-an-integer",
    "name": "Tomato Soup",
    "ingredients": [
        {
            "ingredient_id": 7,
            "ingredient_name": "Tomato",
            "quantity": "2"
        }
    ],
    "time_to_cook": "thirty"
}

valid_paginated_response = {
    "items": [valid_recipe_data],
    "total": 1,
    "links": {
        "self": {"href": "/recipes?page=1"},
        "next": {"href": "/recipes?page=2"},
        "prev": {"href": "/recipes?page=0"}
    }
}

invalid_paginated_response_missing_items = {
    "links": {
        "self": {"href": "/recipes?page=1"}
    }
}


# Test Ingredient Model
def test_ingredient_model_valid():
    ingredient = Ingredient.model_validate(valid_ingredient_data)
    assert ingredient.ingredient_name == "Avocado"
    assert ingredient.quantity == "1 large"
    assert ingredient.ingredient_id == 1


def test_ingredient_model_invalid_missing_field():
    with pytest.raises(ValidationError) as exc_info:
        Ingredient.model_validate({"quantity": "1 large", "ingredient_id": 8})
    assert "ingredient_name" in str(exc_info.value)


def test_ingredient_model_invalid_type():
    with pytest.raises(ValidationError) as exc_info:
        Ingredient.model_validate({"ingredient_name": 123, "quantity": "1 large", "ingredient_id": 9})
    assert "ingredient_name" in str(exc_info.value)


# Test Recipe Model
def test_recipe_model_valid():
    recipe = Recipe.model_validate(valid_recipe_data)
    assert recipe.recipe_id == 171
    assert recipe.name == "Avocado Toast"
    assert len(recipe.ingredients) == 5
    assert recipe.time_to_cook == 10
    assert recipe.meal_type == "breakfast"
    assert recipe.calories == 300
    assert recipe.rating == 4.4
    assert "self" in recipe.links
    assert recipe.ingredients[0].ingredient_id == 1


def test_recipe_model_optional_fields_missing():
    minimal_recipe_data = {
        "recipe_id": 173,
        "name": "Simple Salad",
        "ingredients": [
            {
                "ingredient_id": 10,
                "ingredient_name": "Lettuce",
                "quantity": "1 head"
            }
        ]
    }
    recipe = Recipe.model_validate(minimal_recipe_data)
    assert recipe.recipe_id == 173
    assert recipe.name == "Simple Salad"
    assert recipe.steps is None
    assert recipe.time_to_cook is None
    assert recipe.meal_type is None
    assert recipe.calories is None
    assert recipe.rating is None
    assert recipe.links is None
    assert len(recipe.ingredients) == 1
    assert recipe.ingredients[0].ingredient_id == 10


def test_recipe_model_invalid_missing_required_field():
    with pytest.raises(ValidationError) as exc_info:
        Recipe.model_validate(invalid_recipe_data_missing_name)
    assert "name" in str(exc_info.value)


def test_recipe_model_invalid_field_types():
    with pytest.raises(ValidationError) as exc_info:
        Recipe.model_validate(invalid_recipe_data_wrong_type)
    assert "recipe_id" in str(exc_info.value)
    assert "time_to_cook" in str(exc_info.value)


def test_recipe_serialization():
    recipe = Recipe.model_validate(valid_recipe_data)
    json_data = recipe.model_dump_json()
    assert isinstance(json_data, str)
    parsed = Recipe.model_validate_json(json_data)
    assert parsed == recipe


# Test PaginatedResponse Model
def test_paginated_response_valid():
    paginated = PaginatedResponse(**valid_paginated_response)
    assert isinstance(paginated.items, list)
    assert len(paginated.items) == 1
    assert isinstance(paginated.items[0], dict)  # Since List[Any]
    assert "links" in paginated.dict()
    assert "self" in paginated.links


def test_paginated_response_invalid_missing_items():
    with pytest.raises(ValidationError) as exc_info:
        PaginatedResponse.model_validate(invalid_paginated_response_missing_items)
    assert "items" in str(exc_info.value)


def test_paginated_response_serialization():
    paginated = PaginatedResponse.model_validate(valid_paginated_response)
    json_data = paginated.model_dump_json()
    assert isinstance(json_data, str)
    parsed_paginated = PaginatedResponse.model_validate_json(json_data)
    assert parsed_paginated == paginated
