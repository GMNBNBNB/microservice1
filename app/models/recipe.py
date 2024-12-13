from __future__ import annotations

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class Ingredient(BaseModel):
    ingredient_id: int
    ingredient_name: str
    quantity: str

class Recipe(BaseModel):
    recipe_id: Optional[int] = None
    name: str
    ingredients: List[Ingredient]
    steps: Optional[str] = None
    time_to_cook: Optional[int] = None
    meal_type: Optional[str] = None
    calories: Optional[int] = None
    rating: Optional[float] = None
    links: Optional[Dict[str, Any]] = Field(None, alias="links")

    class Config:
        json_schema_extra  = {
            "example": {
                "name": "Avocado Toast",
                "ingredients": [
                    {
                        "ingredient_id": 148,
                        "ingredient_name": "Avocado",
                        "quantity": "1 large"
                    },
                    {
                        "ingredient_id": 149,
                        "ingredient_name": "Bread",
                        "quantity": "2 slices"
                    },
                    {
                        "ingredient_id": 150,
                        "ingredient_name": "Lime",
                        "quantity": "1/2"
                    },
                    {
                        "ingredient_id": 151,
                        "ingredient_name": "Olive oil",
                        "quantity": "1 tbsp"
                    },
                    {
                        "ingredient_id": 152,
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
        }

class PaginatedResponse(BaseModel):
    items: List[Any]
    links: Dict[str, Any]
