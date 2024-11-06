from fastapi import FastAPI, HTTPException
import asyncio
import httpx
from dotenv import load_dotenv
import os

from models.astral import AstralObject, CellType, ObjectType

load_dotenv()

MEGAVERSE_BASE_URL = os.getenv("MEGAVERSE_BASE_URL")
CANDIDATE_ID = os.getenv("CANDIDATE_ID")

app = FastAPI()

goal_map = []


async def send_request(method: str, endpoint: str, payload: dict):
    url = f"{MEGAVERSE_BASE_URL}/{endpoint}"
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.request(method, url, json=payload)
        response.raise_for_status()
        return response.json()


async def attempt_create_object(object_type: ObjectType, row: int, column: int, max_retries: int = 5, initial_delay: float = 0.5, **kwargs):
    delay = initial_delay
    payload = {"row": row, "column": column,
               "candidateId": CANDIDATE_ID, **kwargs}
    for _ in range(max_retries):
        try:
            await send_request("POST", object_type.value, payload)
            await asyncio.sleep(0.1)
            return True
        except HTTPException as e:
            if e.status_code == 429:
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise e
    print(f"Failed to create {object_type} at ({
          row}, {column}) after multiple attempts.")
    return False


async def run_tasks_with_concurrency(tasks, max_concurrent_tasks: int):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(task):
        async with semaphore:
            return await task
    await asyncio.gather(*(sem_task(task) for task in tasks))


@app.post("/polyanets")
async def create_polyanet(obj: AstralObject):
    return await attempt_create_object(ObjectType.POLYANET, obj.row, obj.column)


@app.delete("/polyanets")
async def delete_polyanet(obj: AstralObject):
    return await send_request("DELETE", ObjectType.POLYANET.value, {"row": obj.row, "column": obj.column, "candidateId": CANDIDATE_ID})


@app.post("/soloons")
async def create_soloon(obj: AstralObject):
    if not obj.color:
        raise HTTPException(
            status_code=400, detail="Color is required for Soloons")
    return await attempt_create_object(ObjectType.SOLOON, obj.row, obj.column, color=obj.color)


@app.delete("/soloons")
async def delete_soloon(obj: AstralObject):
    return await send_request("DELETE", ObjectType.SOLOON.value, {"row": obj.row, "column": obj.column, "candidateId": CANDIDATE_ID})


@app.post("/comeths")
async def create_cometh(obj: AstralObject):
    if not obj.direction:
        raise HTTPException(
            status_code=400, detail="Direction is required for Comeths")
    return await attempt_create_object(ObjectType.COMETH, obj.row, obj.column, direction=obj.direction)


@app.delete("/comeths")
async def delete_cometh(obj: AstralObject):
    return await send_request("DELETE", ObjectType.COMETH.value, {"row": obj.row, "column": obj.column, "candidateId": CANDIDATE_ID})


@app.get("/goal_map")
async def get_goal_map():
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(f"{MEGAVERSE_BASE_URL}/map/{CANDIDATE_ID}/goal")
        response.raise_for_status()
        return response.json()


@app.post("/build_x_shape/")
async def build_x_shape(grid_size: int = 11, left_margin: int = 2, right_margin: int = 2):
    tasks = []
    effective_grid_size = grid_size - left_margin - right_margin
    for i in range(effective_grid_size):
        row = i + left_margin
        col_left = i + left_margin
        col_right = (grid_size - 1) - i - right_margin
        tasks.append(attempt_create_object(ObjectType.POLYANET, row, col_left))
        tasks.append(attempt_create_object(
            ObjectType.POLYANET, row, col_right))
    await run_tasks_with_concurrency(tasks, max_concurrent_tasks=10)
    return {"message": "X shape created successfully with margins"}


@app.post("/build_universe_from_goal_map/")
async def build_universe_from_goal_map():
    global goal_map
    goal_map.clear()
    goal_map = (await get_goal_map())["goal"]
    tasks = []
    for row_index, row in enumerate(goal_map):
        for col_index, cell in enumerate(row):
            cell_type = CellType(cell.split("_")[0])
            if cell_type == CellType.POLYANET:
                tasks.append(attempt_create_object(
                    ObjectType.POLYANET, row_index, col_index))
            elif cell_type == CellType.SOLOON:
                color = cell.split("_")[0].lower()
                tasks.append(attempt_create_object(
                    ObjectType.SOLOON, row_index, col_index, color=color))
            elif cell_type == CellType.COMETH:
                direction = cell.split("_")[0].lower()
                tasks.append(attempt_create_object(
                    ObjectType.COMETH, row_index, col_index, direction=direction))

    await run_tasks_with_concurrency(tasks, max_concurrent_tasks=10)
    return {"message": "Optimized goal map created successfully"}
