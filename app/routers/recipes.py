from fastapi import APIRouter, HTTPException, Query, Request

from app.models.recipe import Recipe, PaginatedResponse
from app.resources.recipe_resource import RecipeResource
from app.services.service_factory import ServiceFactory
from typing import List, Optional

router = APIRouter()


@router.post("/recipes", tags=["recipes"], status_code=201, response_model=Recipe)
async def create_recipe(recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    try:
        new_recipe = res.create_by_key(recipe.dict())
        recipe_data = new_recipe.dict()
        recipe_id = recipe_data.get('recipe_id')

        recipe_data["links"] = {
            "self": {"href": f"/recipes/{recipe_id}"},
            "update": {"href": f"/recipes/{recipe_id}", "method": "PUT"},
            "delete": {"href": f"/recipes/{recipe_id}", "method": "DELETE"}
        }

        return Recipe(**recipe_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create recipe: {e}")

@router.get("/recipes/name/{name}", tags=["recipes"], response_model=Recipe)
async def get_recipe_by_name(name: str) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    result = res.get_by_key(key_value=name, key_field="name")

    if not result:
        raise HTTPException(status_code=404, detail="Recipe not found")

    recipe_data = result.dict()
    recipe_data["links"] = {
        "self": {"href": f"/recipes/name/{name}"},
        "update": {"href": f"/recipes/name/{name}", "method": "PUT"},
        "delete": {"href": f"/recipes/name/{name}", "method": "DELETE"}
    }

    return Recipe(**recipe_data)

@router.get("/recipes/id/{recipe_id}", tags=["recipes"], response_model=Recipe)
async def get_recipe_by_id(recipe_id: int) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    result = res.get_by_key(key_value=recipe_id, key_field="recipe_id")

    if not result:
        raise HTTPException(status_code=404, detail="Recipe not found")

    recipe_data = result.dict()
    recipe_data["links"] = {
        "self": {"href": f"/recipes/id/{recipe_id}"},
        "update": {"href": f"/recipes/id/{recipe_id}", "method": "PUT"},
        "delete": {"href": f"/recipes/id/{recipe_id}", "method": "DELETE"}
    }

    return Recipe(**recipe_data)

@router.put("/recipes/id/{recipe_id}", tags=["recipes"], response_model=Recipe)
async def update_recipe_by_id(recipe_id: int, recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    update_data = recipe.dict(exclude_unset=True)
    result = res.update_by_key(key_value=recipe_id, key_field="recipe_id", data=update_data)
    return result

@router.put("/recipes/name/{name}", tags=["recipes"], response_model=Recipe)
async def update_recipe_by_name(name: str, recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    update_data = recipe.dict(exclude_unset=True)
    result = res.update_by_key(key_value=name, key_field="name", data=update_data)
    return result

@router.delete("/recipes/id/{recipe_id}", tags=["recipes"])
async def delete_recipe_by_id(recipe_id: int):
    res = ServiceFactory.get_service("RecipeResource")
    res.delete_by_key(key_value=recipe_id, key_field="recipe_id")
    return {"message": f"Recipe with id {recipe_id} has been deleted"}

@router.delete("/recipes/name/{name}", tags=["recipes"])
async def delete_recipe_by_name(name: str):
    res = ServiceFactory.get_service("RecipeResource")
    res.delete_by_key(key_value=name, key_field="name")
    return {"message": f"Recipe with name {name} has been deleted"}


@router.get("/recipes", tags=["recipes"], response_model=PaginatedResponse)
async def get_all_recipes(
        request: Request,
        skip: int = Query(0, ge=0, description="Number of records to skip"),
        limit: int = Query(10, ge=1, le=100, description="Number of records to retrieve")
) -> PaginatedResponse:
    """
    Retrieve all recipes with pagination.
    """
    res: RecipeResource = ServiceFactory.get_service("RecipeResource")
    recipes = res.get_all(skip=skip, limit=limit)
    total_count = res.get_total_count()

    base_url = str(request.url).split('?')[0]

    current_query = f"skip={skip}&limit={limit}"
    current_url = f"{base_url}?{current_query}"

    next_skip = skip + limit
    previous_skip = skip - limit if skip - limit >= 0 else 0
    last_skip = ((total_count - 1) // limit) * limit if limit > 0 else 0

    links = {
        "current": {"href": current_url},
        "first": {"href": f"{base_url}?skip=0&limit={limit}"},
        "last": {"href": f"{base_url}?skip={last_skip}&limit={limit}"}
    }

    if next_skip < total_count:
        links["next"] = {"href": f"{base_url}?skip={next_skip}&limit={limit}"}
    if skip > 0:
        links["previous"] = {"href": f"{base_url}?skip={previous_skip}&limit={limit}"}

    updated_recipes = []
    for recipe in recipes:
        recipe_data = recipe.dict()
        recipe_id = recipe_data['recipe_id']
        recipe_data["links"] = {
            "self": {"href": f"/recipes/{recipe_id}"},
            "update": {"href": f"/recipes/{recipe_id}", "method": "PUT"},
            "delete": {"href": f"/recipes/{recipe_id}", "method": "DELETE"}
        }
        updated_recipes.append(Recipe(**recipe_data))

    return PaginatedResponse(items=updated_recipes, links=links)
