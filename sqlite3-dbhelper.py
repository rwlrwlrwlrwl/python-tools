class DBHelper(object):
    def __init__(self):
        self.db_file = DB_FILE
        self._conn = None

    def get_conn(self):
        try:
            if not self._conn:
                self._conn = sqlite3.connect(self.db_file)
        except Exception as e:
            logging.error("sqlite conn db:%s, err:%s" % (self.db_file, e), exc_info=True)
        if self._conn is None:
            raise Exception("sqlite conn db err!")
        return self._conn

    def execute(self, sql, params=None):
        res = None
        logging.info("execute sql:%s, params:%s" % (sql, params))
        try:
            with self.get_conn():
                res = self.get_conn().execute(sql, params if params else [])
        except Exception as e:
            logging.error("sqlite execute sql:%s params:%s err:%s" % (sql, params, e), exc_info=True)
        return res

    def executemany(self, sql, params):
        res = None
        logging.info("executemany sql:%s, params:%s" % (sql, params))
        try:
            with self.get_conn():
                res = self.get_conn().executemany(sql, params)
        except Exception as e:
            logging.error("sqlite executemany sql:%s params:%s err:%s" % (sql, params, e), exc_info=True)
        return res

    def filter(self, sql, params=None):
        res = None
        try:
            res = self.get_conn().execute(sql, params if params else []).fetchall()
        except Exception as e:
            logging.error("sqlite filter sql:%s params:%s err:%s" % (sql, params, e), exc_info=True)
        return res

    def init_table(self):
        sql = "CREATE TABLE suspend_ip(`id` INTEGER PRIMARY KEY AUTOINCREMENT, `ip` VARCHAR(32), " \
              "`status` VARCHAR(16), `customer_id` VARCHAR(36),`customer_name` VARCHAR(100)," \
              "`is_valid` TINYINT,`create_time` DATETIME,`update_time` DATETIME);"
        self.execute(sql)

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception as e:
            logging.error('sqlite close db err:%s' % e, exc_info=True)
