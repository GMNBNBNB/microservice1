from fastapi import APIRouter, HTTPException, Query, Request

from app.models.recipe import Recipe, PaginatedResponse
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

    # 构建基本 URL
    base_url = str(request.url).split('?')[0]

    # 当前页面的查询参数
    current_query = f"skip={skip}&limit={limit}"
    current_url = f"{base_url}?{current_query}"

    # 生成分页链接
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

    # 为每个食谱添加 _links 字段
    updated_recipes = []
    for recipe in recipes:
        recipe_data = recipe.dict()
        recipe_data["links"] = {
            "self": {"href": f"/recipes/{recipe_data['name']}"},
            "update": {"href": f"/recipes/{recipe_data['name']}", "method": "PUT"},
            "delete": {"href": f"/recipes/{recipe_data['name']}", "method": "DELETE"}
        }
        updated_recipes.append(Recipe(**recipe_data))

    return PaginatedResponse(items=updated_recipes, links=links)