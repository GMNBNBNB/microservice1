from fastapi import APIRouter, HTTPException, Query

from app.models.recipe import Recipe
from app.resources.recipe_resource import RecipeResource
from app.services.service_factory import ServiceFactory
from typing import List

router = APIRouter()

@router.post("/recipes", tags=["recipes"], status_code=201)
async def create_recipe(recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    new_recipe = res.create_by_key(recipe.dict())
    return new_recipe

@router.get("/recipes/{name}", tags=["recipes"])
async def get_recipe(name: str) -> dict:
    res = ServiceFactory.get_service("RecipeResource")
    result = res.get_by_key(name)

    if not result:
        return {"message": "Recipe not found"}

    recipe_data = result.dict()
    recipe_data["_links"] = {
        "self": {"href": f"/recipes/{name}"},
        "update": {"href": f"/recipes/{name}", "method": "PUT"},
        "delete": {"href": f"/recipes/{name}", "method": "DELETE"}
    }

    return recipe_data

@router.put("/recipes/{name}", tags=["recipes"])
async def update_recipe(name: str, recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    update_data = recipe.dict(exclude_unset=True)
    result = res.update_by_key(name, update_data)
    return result


@router.delete("/recipes/{name}", tags=["recipes"])
async def delete_recipe(name: str):
    res = ServiceFactory.get_service("RecipeResource")
    res.delete_by_key(name)
    return {"message": f"Recipe with id {name} has been deleted"}

@router.get("/recipes", tags=["recipes"], response_model=List[Recipe])
async def get_all_recipes(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(10, ge=1, le=100, description="Number of records to retrieve")
) -> List[Recipe]:
    """
    Retrieve all recipes with pagination.
    :param skip: Number of records to skip.
    :param limit: Number of records to retrieve.
    :return: List of Recipe objects.
    """
    res = ServiceFactory.get_service("RecipeResource")
    recipes = res.get_all(skip=skip, limit=limit)
    if not recipes:
        raise HTTPException(status_code=404, detail="No recipes found")
    return recipes