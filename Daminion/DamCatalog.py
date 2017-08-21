import sqlite3
import psycopg2
import os
import sys

from Daminion.DamImage import DamImage

class DamCatalog:

    @staticmethod
    def _initMediaList(conn):
        # read and create list where the media format id refers to parent category's key value
        medialist = {}
        cur = conn.cursor()
        cur.execute("SELECT id, parentvalueid, value FROM mediaformat_table")
        rows = cur.fetchall()
        tmp_idx = {}
        for i in range(len(rows)):
            tmp_idx[rows[i][0]] = i
        for i in range(len(rows)):
            j = i
            temp = rows[i][2]
            while rows[j][1] != 0:
                j = tmp_idx[rows[j][1]]
                temp = rows[j][2]
            medialist[rows[i][0]] = temp
        cur.close()
        return medialist

    @staticmethod
    def _initHierList(conn, table):
        # read and create standard list of hierarchical tags
        cur = conn.cursor()
        cur.execute("SELECT id, parentvalueid, value FROM " + table)
        rows = cur.fetchall()
        cur.close()
        tmp_list = {}
        tmp_idx = {}
        for i in range(len(rows)):
            tmp_idx[rows[i][0]] = i
        for i in range(len(rows)):
            j = i
            temp = rows[i][2]
            while rows[j][1] != 0:
                j = tmp_idx[rows[j][1]]
                temp = rows[j][2] + "|" + temp
            tmp_list[rows[i][0]] = temp
        return tmp_list

    @staticmethod
    def _initEventList(conn):
        return DamCatalog._initHierList(conn, "event_table")

    @staticmethod
    def _initPlaceList(conn):
        cur = conn.cursor()
        cur.execute("SELECT id, hierarchylevel, value FROM place_table")
        rows = cur.fetchall()
        cur.close()
        tmplist = {}
        for r in rows:
            tmplist[r[0]] = (r[1], r[2])
        return tmplist

    @staticmethod
    def _initPeopleList(conn):
        return DamCatalog._initHierList(conn, "people_table")

    @staticmethod
    def _initKeywordList(conn):
        return DamCatalog._initHierList(conn, "keywords_table")

    @staticmethod
    def _initCategoryList(conn):
        return DamCatalog._initHierList(conn, "categories_table")

    @staticmethod
    def _initCollectionList(conn):
        return DamCatalog._initHierList(conn, "systemcollection_table")

    @staticmethod
    def _open_db_sqlite(name):
        if os.path.isfile(name):
            return sqlite3.connect(name)
        else:
            sys.stderr.write(name + " is not a valid database file\n")
            sys.exit(-1)

    @staticmethod
    def _open_db_postgres(host, port, name, user, pwd):
        try:
            return psycopg2.connect(host=host, port=port, database=name,
                                                user=user, password=pwd)
        except (Exception, psycopg2.DatabaseError) as error:
            sys.stderr.write(error.args[0])
            sys.exit(-1)

    def __init__(self, host, port, name, user, pwd, sqlite):

        self.catalog = None
        self._dbname = name
        self._counter = 0

        if sqlite:
            self.catalog = self._open_db_sqlite(name)
        else:
            self.catalog = self._open_db_postgres(host, port, name, user, pwd)

    def __del__(self):
        if self.catalog is not None:
            self.catalog.close()

    def initCatalogConstants(self):
        self.MediaList = DamCatalog._initMediaList(self.catalog)
        self.EventList = DamCatalog._initEventList(self.catalog)
        self.PlaceList = DamCatalog._initPlaceList(self.catalog)
        self.PeopleList = DamCatalog._initPeopleList(self.catalog)
        self.KeywordList = DamCatalog._initKeywordList(self.catalog)
        self.CategoryList = DamCatalog._initCategoryList(self.catalog)
        self.CollectionList = DamCatalog._initCollectionList(self.catalog)

    @staticmethod
    def NextImage(cat, session, verbose=0):
        curs = cat.catalog.cursor()
        curs.execute("SELECT id, filename, deleted FROM mediaitems")

        row = curs.fetchone()    # id, filename, deleted
        while row is not None:
            if not bool(row[2]):
                cat._counter += 1
                if verbose > 0:
                    print("\r", "{:7} {:7}: {:60}".format(cat._counter, row[0], row[1]), end="", flush=True)
                yield DamImage(row[0], cat, session)
            row = curs.fetchone()

        curs.close()
