from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class Recipe(BaseModel):
    recipe_id: Optional[int] = None
    name: str
    ingredients: Optional[str] = None
    steps: Optional[str] = None
    kid_friendly: Optional[bool] = None

    class Config:
        json_schema_extra  = {
            "example": {
                "recipe_id": 1,
                "name": "Spaghetti_Bolognese",
                "ingredients": "Spaghetti, tomatoes, minced meat, onions, garlic, olive oil",
                "steps": "1. Boil spaghetti\n2. Cook minced meat with onions and garlic\n3. Add tomatoes and simmer",
                "kid_friendly": True
            }
        }
