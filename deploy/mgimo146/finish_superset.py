"""Run inside Superset to reconnect the existing API usage dataset.

Does not change users, roles or passwords. Clears only result-cache entries.
"""
import os
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database
    for item in db.session.query(Database).all():
        if item.database_name.strip() == 'tradeapi':
            item.set_sqlalchemy_uri(os.environ['TRADEAPI_DSN'].replace('postgresql://', 'postgresql+psycopg2://', 1))
    db.session.commit()
    from superset_cabinet.api_cabinet import _connect
    with _connect() as con, con.cursor() as cur:
        cur.execute('SELECT count(*) FROM plans')
        assert cur.fetchone()[0] >= 4
    import redis
    r = redis.Redis(host='127.0.0.1',port=6379,db=1)
    count = 0
    for key in r.scan_iter(match='superset_data_*',count=200):
        count += r.delete(key)
    print('Cabinet DB connection OK; result cache entries cleared:',count)
