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
MAX_RETRIES = 5
PARTS = 10  # na ile części podzielić

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

async def save_rankings(data, f):
    if not data:
        return
    for item in data.get("rankings", {}).get("data", []):
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

        # folder na pliki
        os.makedirs("output", exist_ok=True)

        # zakresy stron dla 3 plików
        chunk_size = last_page // PARTS
        ranges = []
        for i in range(PARTS):
            start = i * chunk_size + 1
            end = (i + 1) * chunk_size if i < PARTS - 1 else last_page
            ranges.append((start, end))

        # iteracja po częściach
        for idx, (start, end) in enumerate(ranges, 1):
            filename = f"output/ranking_part{idx}.jsonl"
            print(f"▶️ Zapisuję strony {start}–{end} do {filename}")

            with open(filename, "w", encoding="utf-8") as f:
                for page in tqdm_asyncio(range(start, end + 1), desc=f"Część {idx}"):
                    data = await fetch_page(session, page)
                    if data:
                        await save_rankings(data, f)

    print("✅ Zapisano wszystkie strony w katalogu output/ (3 pliki)")

if __name__ == "__main__":
    asyncio.run(main())
