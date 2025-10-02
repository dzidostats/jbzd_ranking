import aiohttp
import asyncio
import json
import os
from tqdm.asyncio import tqdm_asyncio

BASE_URL = "https://m.jbzd.com.pl/ranking/get"
HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0"
}
PER_PAGE = 50
OUTPUT_FILE = "ranking.jsonl"
MAX_RETRIES = 500

async def fetch_page(session, page):
    params = {"page": page, "per_page": PER_PAGE}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(BASE_URL, headers=HEADERS, params=params, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 404:
                    return None  # nie powtarzamy 404
                else:
                    print(f"❌ HTTP {resp.status} dla strony {page}, próba {attempt}")
        except Exception as e:
            print(f"⚠️ Błąd przy stronie {page}, próba {attempt}: {e}")
        await asyncio.sleep(2 * attempt)
    print(f"🚨 Nie udało się pobrać strony {page} po {MAX_RETRIES} próbach")
    return None

async def save_rankings(data, f):
    if not data:
        return
    for item in data.get("rankings", {}).get("data", []):
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

async def main():
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
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

        # sekwencyjne pobieranie z paskiem postępu
        pages = range(2, last_page + 1)
        for page in tqdm_asyncio(pages, desc="Pobieranie stron"):
            data = await fetch_page(session, page)
            if data:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    await save_rankings(data, f)

    print(f"✅ Zapisano wszystkie strony do {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
