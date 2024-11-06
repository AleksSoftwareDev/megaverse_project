from enum import Enum
import asyncio
import httpx


class MegaverseObjectType(Enum):
    POLYANET = "polyanets"
    SOLOON = "soloons"
    COMETH = "comeths"


async def run_tasks_with_concurrency(tasks, max_concurrent_tasks: int):
    """Run tasks with limited concurrency."""
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


async def attempt_create_polyanet(row: int, column: int, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object(MegaverseObjectType.POLYANET, row, column, max_retries, initial_delay)


async def attempt_create_soloon(row: int, column: int, color: str, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object(MegaverseObjectType.SOLOON, row, column, max_retries, initial_delay, color=color)


async def attempt_create_cometh(row: int, column: int, direction: str, max_retries: int = 5, initial_delay: float = 0.5):
    await attempt_create_object(MegaverseObjectType.COMETH, row, column, max_retries, initial_delay, direction=direction)


async def attempt_create_object(object_type: MegaverseObjectType, row: int, column: int, max_retries: int, initial_delay: float, **kwargs):
    delay = initial_delay
    payload = {"row": row, "column": column, "candidateId": CANDIDATE_ID}
    payload.update(kwargs)
    url = f"{MEGAVERSE_BASE_URL}/{object_type.value}"

    for _ in range(max_retries):
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            await asyncio.sleep(0.1)
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit for {object_type.value} at ({
                      row}, {column}), retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise e

    print(f"Failed to create {object_type.value} at ({
          row}, {column}) after multiple attempts.")
