from mysql.connector import pooling


db_pool = pooling.MySQLConnectionPool(
    pool_name="sigma_pool",
    pool_size=20,
    pool_reset_session=True,
    host='localhost',
    user='root',
    password='actowiz',
    database='sigmaldrich_db2'
)

def get_conn():
    return db_pool.get_connection()


def create_table(url_table_name, product_table_name):
    q_url = f"""
        CREATE TABLE IF NOT EXISTS {url_table_name} (
            id          INT PRIMARY KEY AUTO_INCREMENT,
            cat         VARCHAR(100),
            sub_cat     VARCHAR(100),
            sub_sub_cat VARCHAR(100),
            url         VARCHAR(500),
            status      VARCHAR(20) DEFAULT 'pending',
            UNIQUE KEY unique_url (url)
        )"""

    q_prod = f"""
        CREATE TABLE IF NOT EXISTS {product_table_name} (
            id          INT PRIMARY KEY AUTO_INCREMENT,
            productName VARCHAR(255),
            productUrl  VARCHAR(500),
            productKey  VARCHAR(100),
            brand       VARCHAR(100),
            status      VARCHAR(20) DEFAULT 'pending',
            UNIQUE KEY unique_prod_url (productUrl)
        )"""

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(q_url)
    cursor.execute(q_prod)
    conn.commit()
    cursor.close()
    conn.close()


def fetch_url(table_name, status="pending" or "failed"):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT url FROM {table_name} WHERE status=%s", (status,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    for row in rows:
        yield row[0]



def insert_into_db(table_name: str, data: list, batch_size: int = 500):
    if not data:
        return

    rows = list(data)
    cols = ", ".join(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(rows[0]))
    q = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"

    conn = get_conn()
    cursor = conn.cursor()
    total = len(rows)

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        values = [tuple(row.values()) for row in batch]
        cursor.executemany(q, values)
        print(f"  DB Batch {i // batch_size + 1}: inserted {len(batch)} rows ({i + len(batch)}/{total})")

    conn.commit()
    cursor.close()
    conn.close()

def update_url_status(table_name: str, urls: list, status: str = "done"):
    if not urls:
        return

    conn = get_conn()
    cursor = conn.cursor()
    q = f"UPDATE {table_name} SET status=%s WHERE url=%s"
    cursor.executemany(q, [(status, url) for url in urls])
    conn.commit()
    print(f" Updated {len(urls)} URLs → '{status}'")
    cursor.close()
    conn.close()