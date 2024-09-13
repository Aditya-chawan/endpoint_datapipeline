from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import modal
from typing import List
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI()

# Define a Modal stub for batch processing
stub = modal.Stub("task-batch-processing")

# Define the structure of the task using Pydantic
class Task(BaseModel):
    task_name: str
    task_data: dict

# Define the structure of the batch processing result
class BatchProcessResult(BaseModel):
    status: str
    processed_count: int

@stub.function()
def process_task_batch(tasks: List[Task]) -> BatchProcessResult:
    processed_count = sum(1 for task in tasks if logger.info(f"Processing task: {task.task_name}"))
    return BatchProcessResult(status="Batch processed successfully", processed_count=processed_count)

# Use asyncio.Queue for thread-safe task queueing
task_queue = asyncio.Queue()

# API endpoint to collect tasks
@app.post("/submit-task")
async def submit_task(task: Task):
    await task_queue.put(task)
    logger.info(f"Task submitted: {task.task_name}")
    return {"status": "Task added successfully", "task": task}

# Endpoint to process tasks in batch
@app.post("/process-tasks", response_model=BatchProcessResult)
async def process_tasks():
    tasks = []
    while not task_queue.empty():
        tasks.append(await task_queue.get())
    
    if not tasks:
        return BatchProcessResult(status="No tasks to process", processed_count=0)

    logger.info(f"Processing batch of {len(tasks)} tasks")
    # Simulate Modal function call for debugging
    return BatchProcessResult(status="Batch processed successfully", processed_count=len(tasks))

# Define a root endpoint
@app.get("/")
async def root():
    return """
    <h1>Welcome to the Task Processing API</h1>
    <p>Available endpoints:</p>
    <ul>
        <li>POST /submit-task - Submit a new task</li>
        <li>POST /process-tasks - Process queued tasks</li>
    </ul>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)