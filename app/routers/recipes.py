from fastapi import APIRouter

from app.models.recipe import Recipe
from app.resources.recipe_resource import RecipeResource
from app.services.service_factory import ServiceFactory

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

