from store.postgres import PostgresStore
try:
    from storebeatconfig import db_name, db_user, db_password
except Exception as e:
    def pg_store(table):
        return PostgresStore({'table': table, 'name': 'store_beat',
                              'user':  'dameng',
                              'password': 'hello',
                              })
else:
    def pg_store(table):
        return PostgresStore({'table': table, 'name': db_name,
                              'user': db_user,
                              'password': db_password,
                              })
