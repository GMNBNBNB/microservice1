from __future__ import annotations

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class Ingredient(BaseModel):
    ingredient_name: str
    quantity: str

class Recipe(BaseModel):
    recipe_id: int
    name: str
    ingredients: List[Ingredient]
    steps: Optional[str] = None
    time_to_cook: Optional[int] = None
    meal_type: Optional[str] = None
    calories: Optional[int] = None
    rating: Optional[float] = None
    kid_friendly: Optional[bool] = None
    links: Optional[Dict[str, Any]] = Field(None, alias="links")  # 添加 _links 字段

    class Config:
        json_schema_extra  = {
            "example": {
                "recipe_id": 171,
                  "name": "Avocado Toast",
                  "ingredients": [
                    {
                      "ingredient_name": "Avocado",
                      "quantity": "1 large"
                    },
                    {
                      "ingredient_name": "Bread",
                      "quantity": "2 slices"
                    },
                    {
                      "ingredient_name": "Lime",
                      "quantity": "1/2"
                    },
                    {
                      "ingredient_name": "Olive oil",
                      "quantity": "1 tbsp"
                    },
                    {
                      "ingredient_name": "Salt",
                      "quantity": "1/4 tsp"
                    }
                  ],
                  "steps": "1. Toast bread. 2. Mash avocado with lime. 3. Spread on toast and season.",
                  "time_to_cook": 10,
                  "meal_type": "breakfast",
                  "calories": 300,
                  "rating": 4.4,
                  "kid_friendly": True,
                  "_links": {
                    "self": {
                      "href": "/recipes/Avocado Toast"
                    },
                    "update": {
                      "href": "/recipes/Avocado Toast",
                      "method": "PUT"
                    },
                    "delete": {
                      "href": "/recipes/Avocado Toast",
                      "method": "DELETE"
                    }
                }
            }
        }