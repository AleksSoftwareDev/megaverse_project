from fastapi import HTTPException
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import httpx

app = FastAPI(
)

MEGAVERSE_BASE_URL = "https://challenge.crossmint.io/api"
CANDIDATE_ID = "f54424ca-24d6-401d-a112-a0f981f4e1d4"
goal_map = []


class AstralObject(BaseModel):
    row: int
    column: int
    color: Optional[str] = None
    direction: Optional[str] = None


async def send_request(method: str, endpoint: str, payload: dict):
    url = f"{MEGAVERSE_BASE_URL}/{endpoint}"
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.request(method, url, json=payload)
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, detail=response.text
            )
        return response.json()


@app.post("/polyanets")
async def create_polyanet(obj: AstralObject):
    payload = {
        "row": obj.row,
        "column": obj.column,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("POST", "polyanets", payload)


@app.delete("/polyanets")
async def delete_polyanet(obj: AstralObject):
    payload = {
        "row": obj.row,
        "column": obj.column,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("DELETE", "polyanets", payload)


@app.post("/soloons")
async def create_soloon(obj: AstralObject):
    if not obj.color:
        raise HTTPException(
            status_code=400, detail="Color is required for Soloons")
    payload = {
        "row": obj.row,
        "column": obj.column,
        "color": obj.color,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("POST", "soloons", payload)


@app.delete("/soloons")
async def delete_soloon(obj: AstralObject):
    payload = {
        "row": obj.row,
        "column": obj.column,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("DELETE", "soloons", payload)


@app.post("/comeths")
async def create_cometh(obj: AstralObject):
    if not obj.direction:
        raise HTTPException(
            status_code=400, detail="Direction is required for Comeths")
    payload = {
        "row": obj.row,
        "column": obj.column,
        "direction": obj.direction,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("POST", "comeths", payload)


@app.delete("/comeths")
async def delete_cometh(obj: AstralObject):
    payload = {
        "row": obj.row,
        "column": obj.column,
        "candidateId": CANDIDATE_ID
    }
    return await send_request("DELETE", "comeths", payload)


@app.get("/goal_map")
async def get_goal_map():
    url = f"{MEGAVERSE_BASE_URL}/map/{CANDIDATE_ID}/goal"
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url)
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, detail=response.text)
        return response.json()


@app.delete("/polyanets/delete_all/")
async def delete_all_polyanets(grid_size: int = 30):
    for row in range(grid_size):
        for column in range(grid_size):
            success = await attempt_delete(row, column)
            if not success:
                print(f"Failed to delete Polyanet at ({
                      row}, {column}) after retries.")

    return {"message": "All Polyanets deleted (or attempted)"}


async def attempt_delete(row: int, column: int, retries: int = 5, delay: float = 0.5) -> bool:
    try:
        response = await delete_polyanet(AstralObject(row=row, column=column))
        if response:
            await asyncio.sleep(0.1)
        return True
    except HTTPException as e:
        if e.status_code == 404:
            return True
        elif e.status_code == 429 and retries > 0:
            print(f"Rate limit hit at ({row}, {
                  column}), retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            return await attempt_delete(row, column, retries - 1, delay * 2)
        else:
            raise e
    return False


@app.post("/build_x_shape/")
async def build_x_shape(grid_size: int = 11, left_margin: int = 2, right_margin: int = 2):
    effective_grid_size = grid_size - left_margin - right_margin

    for i in range(effective_grid_size):
        # Calculate the adjusted row and column positions based on margins
        row = i + left_margin
        col_left = i + left_margin
        col_right = (grid_size - 1) - i - right_margin
        await attempt_create_polyanet(row, col_left)
        await attempt_create_polyanet(row, col_right)

    return {"message": "X shape created successfully with margins"}


async def attempt_create_polyanet(row: int, column: int, max_retries: int = 10, initial_delay: float = 1) -> bool:
    """Attempts to create a Polyanet with retries and exponential backoff."""
    delay = initial_delay
    for _ in range(max_retries):
        try:
            response = await create_polyanet(AstralObject(row=row, column=column))
            if response:
                await asyncio.sleep(0.1)
            return True
        except HTTPException as e:
            if e.status_code == 429:
                print(f"Rate limit hit at ({row}, {
                      column}), retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise e
    print(f"Failed to create Polyanet at ({row}, {
          column}) after multiple attempts.")
    return False


@app.post("/build_universe_from_goal_map/")
async def build_universe_from_goal_map():
    goal_map.clear
    await fetch_goal_map()
    tasks = []

    for row_index, row in enumerate(goal_map):
        for col_index, cell in enumerate(row):
            if cell == "POLYANET":
                tasks.append(attempt_create_polyanet(row_index, col_index))
            elif "SOLOON" in cell:
                color = cell.split("_")[0].lower()
                tasks.append(attempt_create_soloon(
                    row_index, col_index, color))
            elif "COMETH" in cell:
                direction = cell.split("_")[0].lower()
                tasks.append(attempt_create_cometh(
                    row_index, col_index, direction))

    await run_tasks_with_concurrency(tasks, max_concurrent_tasks=10)
    print("Goal map initialized.")
    print(goal_map)
    return {"message": "Optimized goal map created successfully"}


async def fetch_goal_map():
    global goal_map
    url = f"{MEGAVERSE_BASE_URL}/map/{CANDIDATE_ID}/goal"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    goal_map = data["goal"]
    print(goal_map)


async def run_tasks_with_concurrency(tasks, max_concurrent_tasks: int):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(task):
        async with semaphore:
            return await task

    await asyncio.gather(*(sem_task(task) for task in tasks))


async def attempt_create_polyanet(row: int, column: int, max_retries: int = 10, initial_delay: float = 1):
    """Attempts to create a Polyanet with retries and exponential backoff."""
    await attempt_create_object("polyanets", row, column, max_retries, initial_delay)


async def attempt_create_soloon(row: int, column: int, color: str, max_retries: int = 10, initial_delay: float = 1):
    """Attempts to create a Soloon with retries and exponential backoff."""
    await attempt_create_object("soloons", row, column, max_retries, initial_delay, color=color)


async def attempt_create_cometh(row: int, column: int, direction: str, max_retries: int = 10, initial_delay: float = 1):
    """Attempts to create a Cometh with retries and exponential backoff."""
    await attempt_create_object("comeths", row, column, max_retries, initial_delay, direction=direction)


async def attempt_create_object(object_type: str, row: int, column: int, max_retries: int, initial_delay: float, **kwargs):
    delay = initial_delay
    payload = {"row": row, "column": column, "candidateId": CANDIDATE_ID}
    payload.update(kwargs)
    url = f"{MEGAVERSE_BASE_URL}/{object_type}"

    for _ in range(max_retries):
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            await asyncio.sleep(0.5)
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit for {object_type} at ({row}, {
                      column}), retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise e

    print(f"Failed to create {object_type} at ({
          row}, {column}) after multiple attempts.")


@app.post("/build_custom_matrix/")
async def build_custom_matrix():

    if not goal_map:
        await build_universe_from_goal_map()

    tasks = []

    for row_index, row in enumerate(goal_map):
        for col_index, cell in enumerate(row):
            if cell == "POLYANET":
                tasks.append(attempt_create_polyanet(row_index, col_index))
            elif cell.endswith("SOLOON"):
                color = cell.split("_")[0].lower()
                tasks.append(attempt_create_soloon(
                    row_index, col_index, color))
            elif cell.endswith("COMETH"):
                direction = cell.split("_")[0].lower()
                tasks.append(attempt_create_cometh(
                    row_index, col_index, direction))

    await run_tasks_with_concurrency(tasks, max_concurrent_tasks=2)

    return {"message": "Custom matrix created successfully"}


async def run_tasks_with_concurrency(tasks, max_concurrent_tasks: int):
    """Run tasks with limited concurrency."""
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(task):
        async with semaphore:
            return await task

    await asyncio.gather(*(sem_task(task) for task in tasks))


async def attempt_create_polyanet(row: int, column: int, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object("polyanets", row, column, max_retries, initial_delay)


async def attempt_create_soloon(row: int, column: int, color: str, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object("soloons", row, column, max_retries, initial_delay, color=color)


async def attempt_create_cometh(row: int, column: int, direction: str, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object("comeths", row, column, max_retries, initial_delay, direction=direction)


async def attempt_create_object(object_type: str, row: int, column: int, max_retries: int, initial_delay: float, **kwargs):
    delay = initial_delay
    payload = {"row": row, "column": column, "candidateId": CANDIDATE_ID}
    payload.update(kwargs)
    url = f"{MEGAVERSE_BASE_URL}/{object_type}"

    for _ in range(max_retries):
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            await asyncio.sleep(0.1)
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit for {object_type} at ({row}, {
                      column}), retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise e

    print(f"Failed to create {object_type} at ({
          row}, {column}) after multiple attempts.")
