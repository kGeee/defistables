from db_connection import Postgres
import psycopg2
pg = Postgres(host='cluster.provider-0.prod.ams1.akash.pub',port='31757',user="admin",pw="let-me-in",db="mydb")
conn = pg.connect()
cursor = conn.cursor()
query  = "create table mango_apr (id serial primary key, asset varchar(10), mango_supply decimal, mango_deposit decimal, time time);"

try:
    cursor.execute(query)
    print(cursor.fetchone())
except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cursor.close()
print("execute done")
cursor.close()


drop table anchor_apr; create table anchor_apr (id serial primary key, asset varchar(10), anchor_supply decimal, anchor_borrow decimal, time timestamp, time_uuid varchar(32));
drop table solend_apr; create table solend_apr (id serial primary key, asset varchar(10), solend_supply decimal, solend_borrow decimal, time timestamp, time_uuid varchar(32));
drop table mango_apr; create table mango_apr (id serial primary key, asset varchar(10), mango_supply decimal, mango_borrow decimal, time timestamp, time_uuid varchar(32));
drop table port_apr; create table port_apr (id serial primary key, asset varchar(10), port_supply decimal, port_borrow decimal, time timestamp, time_uuid varchar(32));
drop table jet_apr; create table jet_apr (id serial primary key, asset varchar(10), jet_supply decimal, jet_borrow decimal, time timestamp, time_uuid varchar(32));