#!/usr/bin/env python3

import asyncio
import json
import sys
from functools import lru_cache

import aiohttp

CONN_TIMEOUT = aiohttp.ClientTimeout(total=30)
BASE_URL = 'https://developer.sony.com'
API_PATH = '/api/files'
API_PARAMS = 'tags=xperia-open-source-archives&type=file_download'
BATCH_SIZE = 100
MAX_CONCURRENT = 10
OUTPUT_FILE = 'open-source-archives.json'


def process_item(item):
    key = item.get('key', '')
    content = item.get('content', {})
    post_title = content.get('post_title', '')

    full_url = f'{BASE_URL}{key}' if key else ''

    return {'title': post_title, 'url': full_url}


@lru_cache(maxsize=128)
def build_url(base_url, limit, offset):
    if '?' in base_url:
        return f'{base_url}&limit={limit}&offset={offset}'
    return f'{base_url}?limit={limit}&offset={offset}'


async def parse_sony_json(
    json_file=None, url=None, limit=BATCH_SIZE, offset=BATCH_SIZE
):
    if json_file:
        with open(json_file, 'r') as f:
            data = json.load(f)
    elif url:
        full_url = build_url(url, limit, offset)

        async with aiohttp.ClientSession(timeout=CONN_TIMEOUT) as session:
            async with session.get(full_url) as response:
                if response.status != 200:
                    raise Exception(
                        f'API request failed with status {response.status}'
                    )
                data = await response.json()
    else:
        raise ValueError('Either json_file or url must be provided')

    return [process_item(item) for item in data.get('filesList', [])]


async def fetch_batch(session, url, batch_size, offset):
    full_url = build_url(url, batch_size, offset)

    async with session.get(full_url) as response:
        if response.status != 200:
            raise Exception(f'API request failed with status {response.status}')
        data = await response.json()

    return data, [process_item(item) for item in data.get('filesList', [])]


async def fetch_all_sony_archives(
    url, batch_size=BATCH_SIZE, max_concurrent=MAX_CONCURRENT
):
    all_results = []

    async with aiohttp.ClientSession(timeout=CONN_TIMEOUT) as session:
        data, first_batch = await fetch_batch(session, url, batch_size, 0)
        total_files = data.get('totalFiles', 0)
        all_results.extend(first_batch)

        remaining_offsets = list(range(batch_size, total_files, batch_size))

        if remaining_offsets:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_with_semaphore(offset):
                async with semaphore:
                    _, batch_results = await fetch_batch(
                        session, url, batch_size, offset
                    )
                    return batch_results

            tasks = [
                fetch_with_semaphore(offset) for offset in remaining_offsets
            ]

            batch_results = await asyncio.gather(*tasks)

            for results in batch_results:
                all_results.extend(results)

    return all_results


async def main():
    url = f'{BASE_URL}{API_PATH}?{API_PARAMS}'

    try:
        result = await fetch_all_sony_archives(url)

        with open(OUTPUT_FILE, 'w') as f:
            json.dump(result, f, indent=2)

        print(f'Successfully created {OUTPUT_FILE} with {len(result)} records')
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
