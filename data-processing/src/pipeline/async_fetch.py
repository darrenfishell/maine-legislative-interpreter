import asyncio
import aiohttp
import logging
import os
import json
from io import BytesIO
from datetime import datetime
from tqdm.asyncio import tqdm
from typing import List, Dict, Optional

# Create a logging directory if it doesn't exist
log_directory = 'logs'
os.makedirs(log_directory, exist_ok=True)

# Configure logging with timestamp in filename
log_filename = f"{log_directory}/app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


async def _fetch_data(session: aiohttp.ClientSession, query_url: str, retries: int = 5) -> Optional[Dict]:
    wait_time = 1  # Initial wait time in seconds
    for attempt in range(retries):
        try:
            async with session.get(query_url) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')

                    # Handle JSON or text
                    if 'application/json' in content_type or 'text/' in content_type:
                        response_text = await response.text()
                        return {
                            'query_url': query_url,
                            'text_response': response_text
                        }
                    # Handle other files as binary bytes
                    response_bytes = BytesIO(await response.read())
                    return {
                        'query_url': query_url,
                        'bytes': response_bytes
                    }
                else:
                    logging.error(f"Error fetching {query_url}: Status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching {query_url}: {e}")
            await asyncio.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
    logging.error(f"Max retries reached for {query_url}")
    return None


async def run_in_batches(url_queries: List[str], batch_size: int = 10) -> List[Optional[Dict]]:
    async with aiohttp.ClientSession() as session:
        tasks = []
        results = []
        total_queries = len(url_queries)

        # Create a single tqdm progress bar for all tasks
        with tqdm(total=total_queries, desc="Fetching Data", unit="query") as pbar:
            for query_url in url_queries:
                task = asyncio.create_task(_fetch_data(session, query_url))
                tasks.append(task)

                # Check if we have a full batch to process
                if len(tasks) == batch_size:
                    for completed_task in asyncio.as_completed(tasks):
                        result = await completed_task
                        results.append(result)
                        pbar.update(1)  # Update the progress bar after each task completes
                    tasks = []  # Reset tasks for the next batch

            # Process any leftover tasks after the final batch
            if tasks:
                for completed_task in asyncio.as_completed(tasks):
                    result = await completed_task
                    results.append(result)
                    pbar.update(1)
    return results
