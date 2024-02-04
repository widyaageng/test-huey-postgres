import pickle
import contextlib
import psycopg2

from huey.api import Huey
from huey.storage import BaseSqlStorage
from huey.utils import to_timestamp
from huey.constants import EmptyData

to_bytes = lambda b: bytes(b) if not isinstance(b, bytes) else b

class PostgreStorage(BaseSqlStorage):
    def __init__(self, name, schema_name='huey_schema', **kwargs):
        required_postgre_params = {'database','user','host','password','port'}
        assert len(required_postgre_params - set(kwargs.keys())) == 0, f'Missing required parameters, expect {required_postgre_params}'
        self.database = kwargs.get('database')
        self.user = kwargs.get('user')
        self.host = kwargs.get('host')
        self.password = kwargs.get('password')
        self.port = kwargs.get('port')
        self.schema_name = schema_name
        
        huey_schema =(f'CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {self.user};')
        table_kv = (f'CREATE TABLE IF NOT EXISTS {schema_name}.kv ('
                    'queue character varying NOT NULL, '
                    'key character varying NOT NULL, '
                    'value bytea NOT NULL, '
                    'PRIMARY KEY(queue, key));')
        table_sched = (f'CREATE TABLE IF NOT EXISTS {schema_name}.schedule ('
                    'id SERIAL PRIMARY KEY, queue character varying NOT NULL, '
                    'data bytea NOT NULL, timestamp real NOT NULL);')
        index_sched = ('CREATE INDEX IF NOT EXISTS schedule_queue_timestamp '
                    f'ON {schema_name}.schedule (queue, timestamp);')
        table_task = (f'CREATE TABLE IF NOT EXISTS {schema_name}.task ('
                    'id SERIAL PRIMARY KEY, queue character varying NOT NULL, '
                    'data bytea NOT NULL, priority real NOT NULL DEFAULT 0.0);')
        index_task = (f'CREATE INDEX IF NOT EXISTS task_priority_id ON {schema_name}.task '
                    '(priority DESC, id ASC);')
        self.ddl = [huey_schema, table_kv, table_sched, index_sched, table_task, index_task]
        super(PostgreStorage, self).__init__(**kwargs)

    @contextlib.contextmanager
    def db(self, commit=False, close=False):
        conn = self.conn
        cursor = conn.cursor()
        try:
            # cursor.execute(self.begin_sql)
            yield cursor
        except Exception:
            if commit: conn.rollback()
            raise
        else:
            if commit: conn.commit()
        finally:
            cursor.close()
            if close:
                conn.close()
                self._state.reset()


    def sql(self, query, commit=False, results=False):
        with self.db(commit=commit) as curs:
            curs.execute(query)
            if results:
                return curs.fetchall()
            

    def _create_connection(self):
        conn = psycopg2.connect(database = self.database,user = self.user,host = self.host,password = self.password,port = self.port)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)  # Autocommit mode.
        return conn

    def enqueue(self, data, priority=None):
        self.sql(f"INSERT INTO {self.schema_name}.task (queue, data, priority) VALUES ('{self.name}', {psycopg2.Binary(pickle.dumps(data))}, {priority or 0})", commit=True)

    def dequeue(self):
        with self.db(commit=True) as curs:
            curs.execute(f"SELECT id, data FROM {self.schema_name}.task WHERE queue = '{self.name}' "
                         "ORDER BY priority DESC, id LIMIT 1")
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                data = pickle.loads(data)
                curs.execute(f"DELETE FROM {self.schema_name}.task WHERE id = {tid}")
                if curs.rowcount == 1:
                    return to_bytes(data)

    def queue_size(self):
        return self.sql(f"SELECT COUNT(id) FROM {self.schema_name}.task WHERE queue='{self.name}'", results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = f"SELECT data FROM {self.schema_name}.task WHERE queue='{self.name}' order by priority desc, id"
        if limit is not None:
            sql += f" limit {limit}"

        return [to_bytes(i) for i, in self.sql(sql, results=True)]

    def flush_queue(self):
        self.sql(f"DELETE FROM {self.schema_name}.task WHERE queue='{self.name}'", commit=True)

    def add_to_schedule(self, data, ts, utc):
        self.sql(f"INSERT INTO {self.schema_name}.schedule (queue, data, timestamp) "
                 f"VALUES ('{self.name}', {psycopg2.Binary(pickle.dumps(data))}, {to_timestamp(ts)})", commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            curs.execute(f"SELECT id, data FROM {self.schema_name}.schedule WHERE "
                         f"queue = '{self.name}' AND timestamp <= {to_timestamp(ts)}")
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(to_bytes(pickle.loads(task_data)))
            if id_list:
                curs.execute(f"DELETE FROM {self.schema_name}.schedule WHERE id IN {tuple(id_list)}")
            return data

    def schedule_size(self):
        return self.sql(f"SELECT COUNT(id) FROM {self.schema_name}.schedule WHERE queue='{self.name}'", results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = f"SELECT data FROM {self.schema_name}.schedule WHERE queue='{self.name}' order by timestamp"
        if limit is not None:
            sql += f" limit {limit}"

        return [to_bytes(i) for i, in self.sql(sql, results=True)]

    def flush_schedule(self):
        self.sql(f"DELETE FROM {self.schema_name}.schedule WHERE queue = '{self.name}'", True)

    def put_data(self, key, value, is_result=False):
        self.sql(f"INSERT INTO {self.schema_name}.kv (queue, key, value) "
                 f"VALUES ('{self.name}', '{key}', {psycopg2.Binary(pickle.dumps(value))})"
                 f"ON CONFLICT (queue,key) DO UPDATE SET value = EXCLUDED.value;", True)

    def peek_data(self, key):
        res = self.sql(f"SELECT VALUE FROM {self.schema_name}.kv WHERE queue = '{self.name}' AND key = '{key}'", results=True)
        return to_bytes(res[0][0]) if res else EmptyData

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            curs.execute(f"SELECT value FROM {self.schema_name}.kv WHERE queue = '{self.name}' AND key = '{key}'")
            result = curs.fetchone()
            if result is not None:
                curs.execute(f"DELETE FROM {self.schema_name}.kv WHERE queue='{self.name}' and key = '{key}'")
                if curs.rowcount == 1:
                    return to_bytes(result[0])
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql(f"SELECT 1 FROM {self.schema_name}.kv WHERE queue='{self.name}' and key = '{key}'", results=True))

    def put_if_empty(self, key, value):
        try:
            with self.db(commit=True) as curs:
                curs.execute(f"INSERT OR ABORT INTO {self.schema_name}.kv "
                            f"(queue, key, value) VALUES ('{self.name}', '{key}', {psycopg2.Binary(pickle.dumps(value))})")
        except Exception as exc:
            return False
        else:
            return True

    def result_store_size(self):
        return self.sql(f"SELECT count(*) FROM {self.schema_name}.kv WHERE queue='{self.name}'", results=True)[0][0]

    def result_items(self):
        res = self.sql(f"SELECT key, value FROM {self.schema_name}.kv WHERE queue='{self.name}'", results=True)
        return dict((k, to_bytes(v)) for k, v in res)

    def flush_results(self):
        self.sql(f"DELETE FROM {self.schema_name}.kv WHERE queue='{self.name}'", True)

class PostgreHuey(Huey):
    storage_class = PostgreStorage