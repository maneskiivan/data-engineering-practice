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

    # executing the create table stmts
    sql_stmt_dir = './data/sql/'
    for root, subdirs, sql_stmt_files in os.walk(sql_stmt_dir):
      for file in sql_stmt_files:
        cursor.execute(read_sql_file(sql_stmt_dir + file))

    # copying the csv data into the tables
    sql_stmt_dir = './data/'
    for root, subdirs, sql_stmt_files in os.walk(sql_stmt_dir):
      for file in sql_stmt_files:
        if '.sql' not in file:
          remove_ext = os.path.splitext(file)[0]
          with open(sql_stmt_dir + file, 'r') as f:
            next(f)
            cursor.copy_from(f, remove_ext, sep=',')

    conn.close()

if __name__ == '__main__':
    main()
