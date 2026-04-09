from parser import parser,parse_prod

from db_config import create_table,insert_into_db,fetch_url,update_url_status
import time
import threading

url_table_name="prod_url2"
product_table_name="prod_detail2"
MAX_WORKERS= 5
def main():
    create_table(url_table_name,product_table_name)
    url_data=parser()

    insert_into_db(url_table_name,url_data)

    urls = list(fetch_url(url_table_name))
    results={}

    lock = threading.Lock()

    def fetch(url):
        try:
            data = parse_prod(url)
            with lock:
                results[url] = data or []
        except Exception:
            with lock:
                results[url] = None


    for i in range(0, len(urls), MAX_WORKERS):
        batch = urls[i:i + MAX_WORKERS]
        threads = [threading.Thread(target=fetch, args=(url,)) for url in batch]

        for t in threads: t.start()
        for t in threads: t.join()

        # insert after each batch
        all_products = []
        urls_toUpdate=[]
        for url in batch:
            data = results.get(url)
            if data is not None:
                urls_toUpdate.append(url)
                all_products.extend(data)

        if all_products:
            insert_into_db(product_table_name, all_products)

        if urls_toUpdate:
            update_url_status(url_table_name, urls_toUpdate, status="done")


if __name__=="__main__":
    st=time.time()
    main()
    et=time.time()
    print(et-st)

