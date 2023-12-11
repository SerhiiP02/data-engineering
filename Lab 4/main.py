import psycopg2
from psycopg2 import sql

def execute_script(conn, script):
    cur = conn.cursor()
    cur.execute(script)
    conn.commit()
    cur.close()

def main():
    host = "localhost"
    database = "test1"
    user = "dbuser"
    password = "password"

    conn = psycopg2.connect(host=host, database=database, user=user, password=password)

    # виконання SQL коду
    with open('./scripts/accounts.sql', 'r') as f:
        execute_script(conn, f.read())

    with open('./scripts/products.sql', 'r') as f:
        execute_script(conn, f.read())

    with open('./scripts/transactions.sql', 'r') as f:
        execute_script(conn, f.read())

    # загрузка даних з CSV файлів
    with open('./data/accounts.csv', 'r') as f:
        cur = conn.cursor()
        cur.copy_from(f, 'accounts', sep=',', columns=('customer_id', 'first_name', 'last_name', 'address_1', 'address_2', 'city', 'state', 'zip_code', 'join_date'))
        conn.commit()
        cur.close()

    with open('./data/products.csv', 'r') as f:
        cur = conn.cursor()
        cur.copy_from(f, 'products', sep=',', columns=('product_id', 'product_code', 'product_description'))
        conn.commit()
        cur.close()

    with open('./data/transactions.csv', 'r') as f:
        cur = conn.cursor()
        cur.copy_from(f, 'transactions', sep=',', columns=('transaction_id', 'transaction_date', 'product_id', 'product_code', 'product_description', 'quantity', 'account_id'))
        conn.commit()
        cur.close()

    conn.close()

if __name__ == "__main__":
    main()
