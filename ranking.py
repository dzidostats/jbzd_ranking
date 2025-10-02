import aiohttp
import asyncio
import json
import os
import async_timeout
import time
from tqdm import tqdm

BASE_URL = "https://m.jbzd.com.pl/ranking/get"
HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0"
}
PER_PAGE = 50
OUTPUT_FILE = "ranking.jsonl"

MAX_RETRIES = 5
MIN_DELAY = 0.05   # minimalna przerwa między requestami
MAX_DELAY = 5      # maksymalna przerwa przy problemach

async def fetch_page(session, page):
    params = {"page": page, "per_page": PER_PAGE}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_timeout.timeout(30):
                async with session.get(BASE_URL, headers=HEADERS, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 404:
                        print(f"❌ Strona {page} nie istnieje (404), pomijam")
                        return None
                    else:
                        print(f"❌ HTTP {resp.status} dla strony {page}, próba {attempt}")
        except Exception as e:
            print(f"⚠️ Błąd przy stronie {page}, próba {attempt}: {e}")
        await asyncio.sleep(2 * attempt)
    print(f"🚨 Nie udało się pobrać strony {page} po {MAX_RETRIES} próbach")
    return None

async def save_rankings(data, f):
    for item in data["rankings"]["data"]:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

async def main():
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # pobranie pierwszej strony
        first_data = await fetch_page(session, 1)
        if not first_data:
            print("Nie udało się pobrać strony 1")
            return
        last_page = first_data["rankings"]["last_page"]
        print(f"📌 Do pobrania: {last_page} stron")

        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            await save_rankings(first_data, f)

        delay = MIN_DELAY  # startowa przerwa
        # sekwencyjne pobieranie stron z paskiem postępu
        for page in tqdm(range(2, last_page + 1), desc="Pobieranie stron"):
            start_time = time.time()
            data = await fetch_page(session, page)
            if data:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    await save_rankings(data, f)
                # jeśli sukces, zmniejszamy przerwę, ale nie poniżej MIN_DELAY
                delay = max(MIN_DELAY, delay * 0.9)
            else:
                # jeśli błąd, zwiększamy przerwę, ale nie powyżej MAX_DELAY
                delay = min(MAX_DELAY, delay * 2)
            # przerwa między requestami
            elapsed = time.time() - start_time
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)

    print(f"✅ Zapisano wszystkie strony do {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
