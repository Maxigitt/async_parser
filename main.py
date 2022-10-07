import asyncio
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

import csv
import json
import logging
import aiofiles

ua = UserAgent()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("MAD_SHOP")

headers = {
    "User-Agent": f"{ua.random}"
}


async def _parse_data(response):
    item_description = []
    for page in response:
        soup = BeautifulSoup(page, "lxml")
        product_content = soup.find_all("div", class_="product-content")
        for item in product_content:
            try:
                brand = item.find("div", class_="product-brand").text
                type_ = item.find("div", class_="product-type").text.strip()
                title = item.find("div", class_="link-product").text.strip()
                link = item.find("a").get("href")
                price = item.find("div", class_="product-price").text.rstrip(" ₽").strip()
                sizes = ",".join([size.text.strip() for size in item.find("div", class_="product-size-list")
                                 .find_all("a")])
                item_description.append({
                    "Бренд": brand,
                    "Тип": type_,
                    "Название": title,
                    "Ссылка": link,
                    "Цена": price,
                    "Размеры": sizes
                })
                logger.debug(f"Добавлена позиция {brand}")
            except Exception:
                logger.debug("Нет какого-то аттрибута")
    return item_description


async def _get_data(data_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url=data_url) as response:
            return await response.text()


async def _get_url_from_all_pages(all_pages, data_url):
    all_urls = []
    for page in range(1, all_pages + 1):
        all_urls.append(f"{data_url}page/{page}/")
    return all_urls


async def _get_count_of_pages(res):
    soup = BeautifulSoup(res, "lxml")
    total_pages = int(soup.find("nav", class_="pagination").find_all("a")[-2].text)
    return total_pages


async def _save_to_json(data, name):
    async with aiofiles.open(f"{name}.json", "w", encoding="utf-8") as file:
        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        await file.write(json_data)


def _save_to_csv(data, name):
    with open(f"{name}.csv", "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            (
                "Бренд",
                "Тип",
                "Название",
                "Ссылка",
                "Цена",
                "Размеры"
            )
        )
        for row in data:
            writer.writerow(
                (row["Бренд"],
                 row["Тип"],
                 row["Название"],
                 row["Ссылка"],
                 row["Цена"],
                 row["Размеры"])
            )


async def save_data(parsed_data, file_name, file_type=None):
    if not file_type:
        await _save_to_json(parsed_data, file_name)
        await asyncio.to_thread(_save_to_csv, parsed_data, file_name)
    elif file_type == "json":
        await _save_to_json(parsed_data, file_name)
    elif file_type == "csv":
        await asyncio.to_thread(_save_to_csv, parsed_data, file_name)


async def tasks(url, file_name):
    data = await _get_data(url)
    page_counter = await _get_count_of_pages(data)
    all_urls = await _get_url_from_all_pages(page_counter, url)
    coros = [_get_data(url) for url in all_urls]
    items_data = await asyncio.gather(*coros)
    parsed_items_data = await _parse_data(items_data)
    await save_data(parsed_items_data, file_name,)


def main():
    url, file_name = "https://madshop.ru/category/novinki/", "MAD_SHOP"
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks(url, file_name))


if __name__ == "__main__":
    main()
