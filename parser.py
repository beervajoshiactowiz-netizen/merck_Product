from curl_cffi import requests
from lxml import html
import json, os, gzip
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.dominos.co.in/store-location/',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

PAGE_WORKERS  = 5
MAX_RETRIES   = 3
RETRY_BACKOFF = 5


def parser():
    r = requests.get(
        "https://www.sigmaaldrich.com/US/en",
        impersonate="chrome110",
        timeout=60
    )
    tree = html.fromstring(r.content)
    raw = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
    data = json.loads(raw[0])
    with open("next_data.json", "w") as f:
        json.dump(data, f, indent=2)
    urls = []
    nav = data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {}).get("aemHeaderFooter", {}).get("header", {}).get("topnav", [])[0]

    items = nav.get("items", [])
    for item in items:
        cat = item.get("title")
        cat_url = item.get("url")

        cat_children = item.get("childrens", [])
        if cat_children:
            for sub in cat_children:
                sub_cat = sub.get("title")
                sub_cat_url = sub.get("url")

                sub_child = sub.get("childrens", [])
                if sub_child:
                    for sub_sub in sub_child:
                        title = sub_sub.get("title")
                        url = sub_sub.get("url")
                        urls.append({
                            "cat": cat,
                            "sub_cat": sub_cat,
                            "sub_sub_cat": title,
                            "url": f"https://www.sigmaaldrich.com{url}"
                        })
                else:
                    urls.append({
                        "cat": cat,
                        "sub_cat": sub_cat,
                        "sub_sub_cat": "",
                        "url": f"https://www.sigmaaldrich.com{sub_cat_url}"
                    })
        else:
            urls.append({
                "cat": cat,
                "sub_cat": "",
                "sub_sub_cat": "",
                "url": f"https://www.sigmaaldrich.com{cat_url}"
            })

    return urls


def parse_prod(url):
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    tree = html.fromstring(response.content)
    raw = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
    data = json.loads(raw[0])

    root_query = data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})

    item_path = {}
    for key in root_query:
        if key.startswith("getProductSearchResults") and '"page":1' in key:
            item_path = root_query[key]
            break

    total_pages = item_path.get("metadata", {}).get("numPages")
    if not total_pages:
        return []

    # file_name = url.replace("https://www.sigmaaldrich.com/IN/en/products/", "").replace("/", "_")
    file_name = url.split("/products/")[-1].replace("/", "_")
    os.makedirs("html_pages3", exist_ok=True)
    file_path = f"html_pages3/{file_name}.json.gz"

    if not os.path.exists(file_path):
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            json.dump(data,f)
        print(f"  Saved backup: {file_path}")
    else:
        print(f"  Backup exists, skipping save: {file_path}")

    return extract_data(tree, url, total_pages)


def fetch_page_with_retry(url, page):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(f"{url}?page={page}", headers=headers, timeout=120)
            res.raise_for_status()
            tree = html.fromstring(res.content)
            raw = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
            page_data = json.loads(raw[0])

            root_query = page_data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})
            item_p = {}
            for key in root_query:
                if key.startswith("getProductSearchResults") and f'"page":{page}' in key:
                    item_p = root_query[key]
                    break

            products = []
            for item in item_p.get("items", []):
                productKey = item.get("productKey")
                brand      = item.get("brand", {}).get("key", "").lower()
                products.append({
                    "productName": item.get("name"),
                    "productUrl":  f"https://www.sigmaaldrich.com/SG/en/product/{brand}/{productKey}",
                    "productKey":  productKey,
                    "brand":       brand
                })
            return page, products

        except Exception as e:
            last_err = e
            wait = RETRY_BACKOFF * attempt
            print(f" Page {page} attempt {attempt}/{MAX_RETRIES} failed — retrying in {wait}s")
            time.sleep(wait)

    raise Exception(f"Page {page} failed after {MAX_RETRIES} retries: {last_err}")


def extract_data(tree, url, total_pages):
    raw = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
    data = json.loads(raw[0])
    root_query = data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})

    item_path = {}
    for key in root_query:
        if key.startswith("getProductSearchResults") and '"page":1' in key:
            item_path = root_query[key]
            break

    print("Total Pages:", total_pages)
    products = []


    for item in item_path.get("items", []):
        productKey = item.get("productKey")
        brand      = item.get("brand", {}).get("key", "").lower()
        products.append({
            "productName": item.get("name"),
            "productUrl":  f"https://www.sigmaaldrich.com/SG/en/product/{brand}/{productKey}",
            "productKey":  productKey,
            "brand":       brand
        })

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
            futures = {
                executor.submit(fetch_page_with_retry, url, page): page
                for page in range(2, total_pages + 1)
            }
            for future in as_completed(futures):
                page = futures[future]
                try:
                    _, page_products = future.result()
                    products.extend(page_products)
                    print(f" Page {page}/{total_pages} → {len(page_products)} products")
                except Exception as e:
                    print(f" Page {page} permanently failed: {e}")

    print(len(products))
    return products
