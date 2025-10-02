import aiohttp
import asyncio
import json
import os
import sys
from tqdm.asyncio import tqdm_asyncio

BASE_URL = "https://m.jbzd.com.pl/ranking/get"
HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0"
}
PER_PAGE = 50
MAX_RETRIES = 50
CONCURRENCY = 5  # ile stron równolegle

async def fetch_page(session, page):
    params = {"page": page, "per_page": PER_PAGE}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(BASE_URL, headers=HEADERS, params=params, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 404:
                    return None
                else:
                    print(f"❌ HTTP {resp.status} dla strony {page}, próba {attempt}")
        except Exception as e:
            print(f"⚠️ Błąd przy stronie {page}, próba {attempt}: {e}")
        await asyncio.sleep(2 * attempt)
    print(f"🚨 Nie udało się pobrać strony {page} po {MAX_RETRIES} próbach")
    return None

async def main(start_page, end_page, output_file):
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        all_items = []

        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(page):
            async with sem:
                data = await fetch_page(session, page)
                if data:
                    return data["rankings"]["data"]
                return []

        tasks = [worker(page) for page in range(start_page, end_page + 1)]

        for result in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"Pobieranie {output_file}"):
            items = await result
            all_items.extend(items)

    os.makedirs("output", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for item in all_items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"✅ Zapisano {len(all_items)} rekordów do {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Użycie: python test.py <start_page> <end_page> <output_file>")
        sys.exit(1)
    start_page = int(sys.argv[1])
    end_page = int(sys.argv[2])
    output_file = sys.argv[3]
    asyncio.run(main(start_page, end_page, output_file))
