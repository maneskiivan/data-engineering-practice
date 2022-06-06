import psycopg2

import os


def read_sql_file(path: str) -> str:
  sql_stmt = ''
  with open(path, 'r') as f:
    for line in f.readlines():
      sql_stmt += line

  return sql_stmt




def main():
    host = 'localhost'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    conn.autocommit = True
    cursor = conn.cursor()

    sql_stmt_dir = './data/sql/'
    for root, subdirs, sql_stmt_files in os.walk(sql_stmt_dir):
      for file in sql_stmt_files:
        cursor.execute(read_sql_file(sql_stmt_dir + file))

    conn.close()

if __name__ == '__main__':
    main()
