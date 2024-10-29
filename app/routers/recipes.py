from fastapi import APIRouter, HTTPException, Query

from app.models.recipe import Recipe
from app.resources.recipe_resource import RecipeResource
from app.services.service_factory import ServiceFactory
from typing import List

router = APIRouter()


@router.post("/recipes", tags=["recipes"], status_code=201, response_model=Recipe)
async def create_recipe(recipe: Recipe) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    try:
        new_recipe = res.create_by_key(recipe.dict())
        recipe_data = new_recipe.dict()  # 确保 result 有 dict() 方法返回字典
        recipe_data["links"] = {
            "self": {"href": f"/recipes/{recipe_data.get("name")}"},
            "update": {"href": f"/recipes/{recipe_data.get("name")}", "method": "PUT"},
            "delete": {"href": f"/recipes/{recipe_data.get("name")}", "method": "DELETE"}
        }

        return Recipe(**recipe_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create recipe: {e}")

@router.get("/recipes/{name}", tags=["recipes"], response_model=Recipe)
async def get_recipe(name: str) -> Recipe:
    res = ServiceFactory.get_service("RecipeResource")
    result = res.get_by_key(name)

    if not result:
        raise HTTPException(status_code=404, detail="Recipe not found")

    recipe_data = result.dict()  # 确保 result 有 dict() 方法返回字典
    recipe_data["links"] = {
        "self": {"href": f"/recipes/{name}"},
        "update": {"href": f"/recipes/{name}", "method": "PUT"},
        "delete": {"href": f"/recipes/{name}", "method": "DELETE"}
    }

    return Recipe(**recipe_data)  # 返回 Pydantic 模型实例

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
    :return: List of Recipe objects with hypermedia links.
    """
    res = ServiceFactory.get_service("RecipeResource")
    recipes = res.get_all(skip=skip, limit=limit)

    # 如果没有找到食谱，返回空列表而不是 404
    if not recipes:
        return []

    # 为每个食谱添加 _links 字段并转换为 Recipe 模型
    updated_recipes = []
    for recipe in recipes:
        # 将 Pydantic 模型转换为字典
        recipe_data = recipe.dict() if hasattr(recipe, 'dict') else recipe.copy()

        # 添加 _links 字段
        recipe_data["links"] = {
            "self": {"href": f"/recipes/{recipe_data['name']}"},
            "update": {"href": f"/recipes/{recipe_data['name']}", "method": "PUT"},
            "delete": {"href": f"/recipes/{recipe_data['name']}", "method": "DELETE"}
        }

        # 将字典转换回 Recipe 模型
        updated_recipe = Recipe(**recipe_data)
        updated_recipes.append(updated_recipe)

    return updated_recipes