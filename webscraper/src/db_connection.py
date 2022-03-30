import psycopg2

class Postgres:
   def __init__(self, host, port, user, pw, db):
      self.host = host
      self.port = port
      self.user = user
      self.pw = pw
      self.db = db

   def connect(self):
      return psycopg2.connect(database=self.db, user=self.user, password=self.pw, host=self.host, port = self.port)

   def execute_many(self, conn, df, table):
      """
      Using cursor.executemany() to insert the dataframe
      """
      # Create a list of tupples from the dataframe values
      tuples = [tuple(x) for x in df.to_numpy()]
      # Comma-separated dataframe columns
      cols = ','.join(list(df.columns))
      # SQL quert to execute
      query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,TIMESTAMP %%s, %%s)" % (table, cols)
      print(table, cols, tuples)
      cursor = conn.cursor()
      try:
         cursor.executemany(query, tuples)
         conn.commit()
      except (Exception, psycopg2.DatabaseError) as error:
         print("Error: %s" % error)
         conn.rollback()
         cursor.close()
         return 1
      print("execute_many() done")
      cursor.close()
conn = psycopg2.connect(database="mydb", user='admin',password='let-me-in', host='cluster.provider-0.prod.ams1.akash.pub',port='31757')







