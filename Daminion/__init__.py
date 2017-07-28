import sys
import sqlite3
import psycopg2

global VerboseOutput

imagefiletypekey = ["%7jnbapuim4$lwk:d45bb3b6-b441-435c-a3ec-b27d067b7c53",
                    "%7jnbapuim4$lwk:343f9214-79a7-4b58-96a3-b7838e3e37ee"]  # magic keys from database


class DamImage:

    def _getmultivaluetags(self, tag, table, listname):
        cur = self._db.catalog.cursor()
        cur.execute("SELECT id_value FROM " + table + " WHERE id_mediaitem=" + str(self.__id) + " ORDER BY id_value")
        rows = cur.fetchall()
        setattr(self, tag, [])
        for r in rows:
            if r[0] in getattr(self._db, listname):
                getattr(self, tag).append(getattr(self._db, listname)[r[0]])
            else:
                getattr(self, tag).append("–ERROR–")
                print("***ERROR: Invalid {} in id: {}, image: {}\\{}".format(
                        tag, self.__id, self._ImagePath, self.ImageName))
        cur.close()

    def __init__(self, db, img_id):

        self._db = db
        self.__id = img_id

        # get full filename
        cur = db.catalog.cursor()
        cur.execute("SELECT filename, relativepath FROM files WHERE id_mediaitem=" + str(img_id))
        row = cur.fetchone()
        if row is None:
            self.IsDeleted = True
            self.IsImage = False
            cur.execute("SELECT filename FROM mediaitems WHERE id=" + str(img_id))
            row = cur.fetchone()
            if row is None:
                self._ImageName = "<empty>"
            else:
                self._ImageName = row[0]
            print("***ERROR: No corresponding file entry for {} (id: {})".format(self._ImageName, img_id))
            cur.close()
            return

        self._ImageName = row[0]
        self._ImagePath = row[1]
#        parts = self.ImageName.split('.')
#        filetype = parts[len(parts)-1].lower()
#        self.IsImage = filetype in imagefiletypes

        # get Event and Media format (id)
        cur.execute("SELECT id_event, id_mediaformat, deleted FROM mediaitems WHERE id=" + str(img_id))
        row = cur.fetchone()
        self.IsDeleted = bool(row[2])
        if row[0] in self._db.EventList:
            self.Event = self._db.EventList[row[0]]
        else:
            self.Event = "–ERROR–"
            print("***ERROR: Invalid Event in id: {}, image: {}\\{}".format(self.__id, self._ImagePath,
                                                                            self._ImageName))
        if row[1] in self._db.MediaList:
            self.IsImage = self._db.MediaList[row[1]] in imagefiletypekey
        else:
            self.IsImage = False
            print("***ERROR: Invalid Media format in id: {}, image: {}\\{}".format(self.__id, self._ImagePath,
                                                                                   self._ImageName))

        # get Place
        cur.execute("SELECT id_value FROM place_file WHERE id_mediaitem=" + str(img_id))
        rows = cur.fetchall()
        placelist = []
        for r in rows:
            placelist.append(self._db.PlaceList[r[0]])
        placelist.sort()
        self.Place = ""
        for p in placelist:
            if self.Place != "":
                self.Place += '|'
            self.Place += p[1]

        # get GPS
        cur.execute("SELECT gpslatitude, gpslongitude, gpsaltitude FROM image WHERE id_mediaitem=" + str(img_id))
        row = cur.fetchone()
        if row is None:
            lat = 0.0
            long = 0.0
            alt = 0.0
        else:
            lat = row[0]
            long = row[1]
            alt = row[2]
        self.GPS = "{}N {}E {}m".format(lat, long, alt)
        cur.close()

        # get list of People, Keywords and Categories
        self._getmultivaluetags("People", "people_file", "PeopleList")
        self._getmultivaluetags("Keywords", "keywords_file", "KeywordList")
        self._getmultivaluetags("Categories", "categories_file", "CategoryList")

    def __str__(self):
        return self.ImageName

    @property
    def ImageName(self):
        line = ""
        if self._db.FullPath:
            line += self._ImagePath + "\\"
        line += self._ImageName
        if self._db.PrintID:
            line += " ({})".format(self.__id)
        return line

    def _linked(self, select, where):
        tmp_list = []
        cur = self._db.catalog.cursor()
        cur.execute("SELECT " + select + " FROM mediaitems_link WHERE " + where + "=" + str(self.__id))
#       if VerboseOutput:
#           print("Linked {} count {}: {}".format(where, self.ImageName, cur.rowcount))
        row = cur.fetchall()
        for r in row:
            img = DamImage(self._db, r[0])
            if not img.IsDeleted and img.IsImage:
                tmp_list.append(img)
        cur.close()
        return tmp_list

    def _topItem(self):
        cur = self._db.catalog.cursor()
        cur.execute("SELECT id_topmediaitemstack FROM mediaitems WHERE id=" + str(self.__id))
        row = cur.fetchone()
        cur.close()
        if row[0] == self.__id or row[0] is None:
            return []
        else:
            img = DamImage(self._db, row[0])
            if not img.IsDeleted and img.IsImage:
                return [img]
            else:
                return []

    def _bottomItems(self):
        tmp_list = []
        cur = self._db.catalog.cursor()
        cur.execute("SELECT id FROM mediaitems WHERE id_topmediaitemstack=" + str(self.__id))
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            if r[0] != self.__id:
                img = DamImage(self._db, r[0])
                if not img.IsDeleted and img.IsImage:
                    tmp_list.append(img)
        return tmp_list

    def LinkedTo(self):
        if self._db.useGroup:
            return self._topItem()
        else:
            return self._linked("id_tomediaitem", "id_frommediaitem")

    def LinkedFrom(self):
        if self._db.useGroup:
            return self._bottomItems()
        else:
            return self._linked("id_frommediaitem", "id_tomediaitem")

    def GetTags(self, tag):
        tags = getattr(self, tag)
        return tags

    def SameSingleValueTag(self, other, tag):
        mytag = self.GetTags(tag)
        othertag = other.GetTags(tag)
        line = self.ImageName + "\t<>\t" + other.ImageName + "\t" + tag
        orig_len = len(line)
        if mytag != othertag:
            line += "\t'" + mytag + "'\t<>\t'" + othertag + "'"
        if orig_len < len(line) or VerboseOutput:
            print(line)

    def SameMultiValueTags(self, d, other, tag):
        mytags = self.GetTags(tag)
        othertags = other.GetTags(tag)
        line = self.ImageName + "\t" + d + "\t" + other.ImageName + "\t" + tag + "\t"
        orig_len = len(line)
        for tagvalue in othertags:
            if tagvalue not in mytags and tagvalue != "":
                if len(line) > orig_len:
                    line += ", "
                line += "'" + tagvalue + "'"
        if orig_len < len(line) or VerboseOutput:
            print(line)


class DamCatalog:
    _damTags = ["Event", "Place", "GPS", "People", "Keywords", "Categories"]
    EventList = {}
    PlaceList = {}
    PeopleList = {}
    KeywordList = {}
    CategoryList = {}
    MediaList = {}

    def _initMediaList(self):
        # read and create list where the media format id refers to parent category's key value
        cur = self.catalog.cursor()
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
            self.MediaList[rows[i][0]] = temp
        cur.close()

    def _initHierList(self, listname, table):
        # read and create standard list of hierarchical tags
        cur = self.catalog.cursor()
        cur.execute("SELECT id, parentvalueid, value FROM " + table)
        rows = cur.fetchall()
        tmp_idx = {}
        for i in range(len(rows)):
            tmp_idx[rows[i][0]] = i
        for i in range(len(rows)):
            j = i
            temp = rows[i][2]
            while rows[j][1] != 0:
                j = tmp_idx[rows[j][1]]
                temp = rows[j][2] + "|" + temp
            getattr(self,listname)[rows[i][0]] = temp
        cur.close()

    def _initEventList(self):
        self._initHierList("EventList", "event_table")

    def _initPlaceList(self):
        cur = self.catalog.cursor()
        cur.execute("SELECT id, hierarchylevel, value FROM place_table")
        rows = cur.fetchall()
        for r in rows:
            self.PlaceList[r[0]] = (r[1], r[2])
        cur.close()

    def _initPeopleList(self):
        self._initHierList("PeopleList", "people_table")

    def _initKeywordList(self):
        self._initHierList("KeywordList", "keywords_table")

    def _initCategoryList(self):
        self._initHierList("CategoryList", "categories_table")

    def __init__(self, host, port, name, user, pwd, sqlite):
        self.catalog = None
        self.__dbname = name
        self.__counter = 0
        try:
            if sqlite:
                self.catalog = sqlite3.connect(name)
            else:
                self.catalog = psycopg2.connect(host=host, port=port, database=name,
                                                user=user, password=pwd)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(-1)

        self._initMediaList()
        self._initEventList()
        self._initPlaceList()
        self._initPeopleList()
        self._initKeywordList()
        self._initCategoryList()

        if VerboseOutput:
            print("Database", self.__dbname, "opened and datastructures initialized.")

    def __del__(self):
        if self.catalog is not None:
            self.__scan.close()
            self.catalog.close()

    def __str__(self):
        return "{}".format(self.__dbname)

    def InitSearch(self):
        self.__scan = self.catalog.cursor()
        self.__scan.execute("SELECT id, filename, deleted FROM mediaitems")
        if VerboseOutput:
            print("Number of files: ", self.__scan.rowcount)

    def NextImage(self):
        row = self.__scan.fetchone()    #  id, filename, deleted
        while row is not None and row[2]:
            row = self.__scan.fetchone()
        if row is not None:
            self.__counter += 1
            if VerboseOutput:
                print("\r", self.__counter, row, end="", flush=True)
            self.__image = DamImage(self, row[0])
            return True
        else:
            return False

    def ScanCatalog(self):
        self.InitSearch()
        print("ImageA\tDir\tImageB\tTag\tValueA/Missing A\t\tValueB")
        while self.NextImage():
            if not self.__image.IsImage or self.__image.IsDeleted:
                continue
            ToList = self.__image.LinkedTo()
            FromList = self.__image.LinkedFrom()
            if VerboseOutput and (ToList != [] or FromList != []):
                print("")
            for tag in self._damTags:
                if tag in ["Event", "Place", "GPS"]:    #  single value tags
                    for img in ToList:
                        self.__image.SameSingleValueTag(img, tag)
                else:                                   #  multi value tags
                    for img in ToList:
                        self.__image.SameMultiValueTags(">", img, tag)
                    for img in FromList:
                        self.__image.SameMultiValueTags("<", img, tag)
        return
