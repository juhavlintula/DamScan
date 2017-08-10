#
#   Copyright Juha Lintula (juha.v.lintula@gmail.com), 2017
#
#
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#

import sys
import os
import datetime
import argparse
import configparser
import sqlite3
import psycopg2
import re

__version__ = "1.0.9"
__doc__ = "This program is checking if all the linked or grouped items in a Daminion catalog have same tags."

#   Version history
#   0.8     – first released version
#   0.9     – added option to use groups instead of links
#           – changed the separator in hierarchical tags to '|' as in Daminion
#           - changed to check against Daminion media format categories Camera RAW and Images
#   0.91    – improved detecting of deleted images
#   0.92    – added support to sqlite (= standalone) catalogs
#             option -l/--sqlite, use full pathname for the catalog
#   0.93    – added option for full path name (-f/--fullpath)
#           – fixed verbose output like Wilfried suggested
#           – added checks for missing database entries
#   0.94    – added option -i/--id to print the database ids
#   0.95    – improved verbose output
#   0.96    – fixed bug in fetching the corresponding file entry
#   0.97    – fixed verbose output, did some refactoring – "beta release"
#   0.98    – added option -o/--output to specify output file, verbose output goes to stdout
#   0.99    – added options
#             -x/--exclude tag value file to exclude list of values from comparison
#             -b/--basename compare filename
#             -t/--tags specify tag categories to be compared
#   1.0     – minor usability improvements
#           – added possibility to exclude branches in exclude file
#           - added option -y/--only as an opposite to -x
#   1.0.1   – refactoring
#   1.0.2   – refactoring
#   1.0.3   – corrected bug when reporting opening a non-existing server catalog
#   1.0.4   – refactoring
#   1.0.5   – refactoring
#   1.0.6   – refactoring and added -a/--acknowledged option
#   1.0.7   – added a possibility to have multiple lines for same tag category-image pairs & bug fixing
#   1.0.8   – minor bug fixes
#   1.0.9   – minor bug fixes

VerboseOutput = 0
imagefiletypekey = ["%7jnbapuim4$lwk:d45bb3b6-b441-435c-a3ec-b27d067b7c53",
                    "%7jnbapuim4$lwk:343f9214-79a7-4b58-96a3-b7838e3e37ee"]  # magic keys from database


class FilterTags(configparser.ConfigParser):

    def _has_option(self, section, option):
        if option == '':
            return False

        o_part = option.split('|')
        opt = ""
        for o in o_part:
            opt += o
            if configparser.ConfigParser.has_option(self, section, opt):
                return True
            opt += '|'
        return False

    def _has_no_option(self, section, option):
        return not self._has_option(section, option)

    def __init__(self, filterfile, include=False):
        configparser.ConfigParser.__init__(self, allow_no_value=True)
        self.optionxform = str
        self.has_option = self._has_option
        if filterfile is not None:
            if self.read(filterfile, encoding='utf-8') == []:
                sys.stderr.write("File " + filterfile + " specified with -x|-y doesn't exist. Option ignored.\n")
            elif include:
                self.has_option = self._has_no_option

#       For verbose print the filter list
        if VerboseOutput > 0:
            line = "Tags that are"
            if include:
                line += " not"
            line += " filtered are:"
            print(line)
            for s in self.sections():
                print("[{}]".format(s))
                for o in self.options(s):
                    print(o)
            print("")


class DamImage:

    @staticmethod
    def _getMultiValueTags(cur, img_id, tag, table, filename, valuelist):
        cur.execute("SELECT id_value FROM " + table + " WHERE id_mediaitem=" + str(img_id) + " ORDER BY id_value")
        rows = cur.fetchall()
        tmp_list = []
        for r in rows:
            if r[0] in valuelist:
                tmp_list.append(valuelist[r[0]])
            else:
                tmp_list.append("–ERROR–")
                sys.stderr.write("***ERROR: Invalid {} in id: {}, image: {}\n".format(tag, img_id, filename))
        return tmp_list

    @staticmethod
    def _get_filename(cur, img_id):
        cur.execute("SELECT filename, relativepath FROM files WHERE id_mediaitem=" + str(img_id))
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT filename FROM mediaitems WHERE id=" + str(img_id))
            row = cur.fetchone()
            if row is None:
                name = "<empty>"
            else:
                name = row[0]
            sys.stderr.write("***ERROR: No corresponding file entry for {} (id: {})\n".format(name, img_id))
            return name, "<empty>", True
        else:
            return row[0], row[1], False

    @staticmethod
    def _get_mediaitems_attr(cur, img_id, filename, medialist, eventlist):

        global imagefiletypekey

        cur.execute("SELECT id_event, id_mediaformat, deleted FROM mediaitems WHERE id=" + str(img_id))
        row = cur.fetchone()
        isdeleted = row is None or bool(row[2])
        if isdeleted:
            return "", False, isdeleted
        if row[0] in eventlist:
            event = eventlist[row[0]]
        else:
            event = "–ERROR–"
            sys.stderr.write("***ERROR: Invalid Event in id: {}, image: {}\n".format(img_id, filename))
        if row[1] in medialist:
            isimage = medialist[row[1]] in imagefiletypekey
        else:
            isimage = False
            sys.stderr.write("***ERROR: Invalid Media format in id: {}, image: {}\n".format(img_id, filename))
        return event, isimage, isdeleted

    @staticmethod
    def _get_place(cur, img_id, filename, places):
        cur.execute("SELECT id_value FROM place_file WHERE id_mediaitem=" + str(img_id))
        rows = cur.fetchall()
        tmp_placelist = []
        for r in rows:
            tmp_placelist.append(places[r[0]])
        tmp_placelist.sort()
        placelist = ""
        for p in tmp_placelist:
            if placelist != "":
                placelist += '|'
            placelist += p[1]
        return placelist

    @staticmethod
    def _get_GPS(cur, img_id):
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
        GPSstring = "{}N {}E {}m".format(lat, long, alt)
        return GPSstring

    def __init__(self, img_id, db, session):
        self._db = db
        self._id = img_id
        self._session = session

        cur = db.catalog.cursor()
        self._ImageName, self._ImagePath, err_flag = self._get_filename(cur, img_id)
        if err_flag:
            self.IsDeleted = True
            self.IsImage = False
        filename = self._ImagePath + "\\" + self._ImageName
        self.Event, self.IsImage, self.IsDeleted = self._get_mediaitems_attr(cur, img_id, filename,
                                                                             db.MediaList, db.EventList)
        if self.IsDeleted:
            self.Place = ""
            self.GPS = ""
            self.People = []
            self.Keywords =[]
            self.Categories = []
        else:
            self.Place = self._get_place(cur, img_id, filename, db.PlaceList)
            self.GPS = self._get_GPS(cur, img_id)

            # get list of People, Keywords and Categories
            self.People = self._getMultiValueTags(cur, img_id, "People", "people_file", filename, db.PeopleList)
            self.Keywords = self._getMultiValueTags(cur, img_id, "Keywords", "keywords_file", filename, db.KeywordList)
            self.Categories = self._getMultiValueTags(cur, img_id, "Categories", "categories_file", filename,
                                                  db.CategoryList)
        cur.close()

    def __str__(self):
        return self.ImageName

    def __repr__(self):
        return self.ImageName

    @property
    def ImageName(self):
        line = ""
        if self._session.fullpath:
            line += self._ImagePath + "\\"
        line += self._ImageName
        if self._session.print_id:
            line += " ({})".format(self._id)
        return line

    @property
    def basename(self):
        name = self._ImageName.rsplit('.', maxsplit=1)[0]
        if self._session.comp_name is not None:
            for c in self._session.comp_name:
                tmp = name.rsplit(c, maxsplit=1)[0]
                if len(tmp) < 8:
                    break
                name = tmp
        return name

    #  Ignore deleted entries and items that are not images
    @property
    def isvalid(self):
        return not self.IsDeleted and self.IsImage

    def linked(self, select, where):
        tmp_list = []
        cur = self._db.catalog.cursor()
        cur.execute("SELECT " + select + " FROM mediaitems_link WHERE " + where + "=" + str(self._id))
#        if VerboseOutput:
#            print("Linked {} count {}: {}".format(where, self.ImageName, cur.rowcount))
        row = cur.fetchall()
        for r in row:
            img = DamImage(r[0], self._db, self._session)
            if img.isvalid:
                tmp_list.append(img)
        cur.close()
        return tmp_list

    def top_item(self):
        cur = self._db.catalog.cursor()
        cur.execute("SELECT id_topmediaitemstack FROM mediaitems WHERE id=" + str(self._id))
        row = cur.fetchone()
        cur.close()
        if row[0] == self._id or row[0] is None:
            return []
        else:
            img = DamImage(row[0], self._db, self._session)
            if img.isvalid:
                return [img]
            else:
                return []

    def bottom_items(self):
        tmp_list = []
        cur = self._db.catalog.cursor()
        cur.execute("SELECT id FROM mediaitems WHERE id_topmediaitemstack=" + str(self._id))
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            if r[0] != self._id:
                img = DamImage(r[0], self._db, self._session)
                if img.isvalid:
                    tmp_list.append(img)
        return tmp_list

    def GetTags(self, tag):
        tags = getattr(self, tag)
        return tags

    def SameSingleValueTag(self, other, tagcat, filter_list, filter_pairs):
        mytag = self.GetTags(tagcat)
        othertag = other.GetTags(tagcat)
        pair = (tagcat, self._id, other._id) in filter_pairs
        if filter_list.has_option(tagcat, mytag) or filter_list.has_option(tagcat, othertag) or pair:
            return

        line = self.ImageName + "\t<>\t" + other.ImageName + "\t" + tagcat
        orig_len = len(line)
        if mytag != othertag:
            line += "\t'" + mytag + "'\t<>\t'" + othertag + "'"
        if orig_len < len(line):
            self._session.outfile.write(line + "\n")

    def SameMultiValueTags(self, d, other, tagcat, filter_list, filter_pairs):
        mytags = self.GetTags(tagcat)
        othertags = other.GetTags(tagcat)
        line = self.ImageName + "\t" + d + "\t" + other.ImageName + "\t" + tagcat + "\t"
        orig_len = len(line)
        for tagvalue in othertags:
            pair = (tagcat, self._id, other._id, tagvalue) in filter_pairs
            if tagvalue not in mytags and not filter_list.has_option(tagcat, tagvalue) and tagvalue != "" and not pair:
                if len(line) > orig_len:
                    line += ", "
                line += "'" + tagvalue + "'"
        if orig_len < len(line):
            self._session.outfile.write(line + "\n")


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

        self.MediaList = DamCatalog._initMediaList(self.catalog)
        self.EventList = DamCatalog._initEventList(self.catalog)
        self.PlaceList = DamCatalog._initPlaceList(self.catalog)
        self.PeopleList = DamCatalog._initPeopleList(self.catalog)
        self.KeywordList = DamCatalog._initKeywordList(self.catalog)
        self.CategoryList = DamCatalog._initCategoryList(self.catalog)

        if VerboseOutput > 0:
            print("Database", self._dbname, "opened and datastructures initialized.")

    def __del__(self):
        if self.catalog is not None:
            self.catalog.close()

    def NextImage(self, session):
        curs = self.catalog.cursor()
        curs.execute("SELECT id, filename, deleted FROM mediaitems")

        row = curs.fetchone()    # id, filename, deleted
        while row is not None:
            if not bool(row[2]):
                self._counter += 1
                if VerboseOutput > 0:
                    print("\r", "{:7} {:7}: {:60}".format(self._counter, row[0], row[1]), end="", flush=True)
                yield DamImage(row[0], self, session)
            row = curs.fetchone()

        curs.close()

    def ScanCatalog(self, session):
        session.outfile.write("ImageA\tDir\tImageB\tTag\tValueA/Missing A\t\tValueB\n")
        taglist = session.tag_cat_list
        exclude = session.filter_list
        for curr_img in self.NextImage(session):
            if not curr_img.isvalid:
                continue
            if session.group:       # by groups
                ToList = curr_img.top_item()
                FromList = curr_img.bottom_items()
            else:                   # by links
                ToList = curr_img.linked("id_tomediaitem", "id_frommediaitem")
                FromList = curr_img.linked("id_frommediaitem", "id_tomediaitem")
            if ToList == [] and FromList == []:
                continue
            if VerboseOutput > 1:
                print("\n{}\t{}\t{}".format(FromList, curr_img, ToList))
            if session.comp_name is not None:
                for lst in ToList, FromList:
                    for f in lst:
                        if curr_img.basename != f.basename and ("Name", curr_img._id, f._id) not in session.filter_pairs:
#                        if curr_img.basename != f.basename:
                            session.outfile.write(curr_img.ImageName + "\t<>\t" + f.ImageName + "\tName\n")
            for tag in taglist:
                if tag in ["Event", "Place", "GPS"]:    # single value tags
                    for img in ToList:
                        curr_img.SameSingleValueTag(img, tag, exclude, session.filter_pairs)
                else:                                   # multi value tags
                    for img in ToList:
                        curr_img.SameMultiValueTags(">", img, tag, exclude, session.filter_pairs)
                    for img in FromList:
                        curr_img.SameMultiValueTags("<", img, tag, exclude, session.filter_pairs)


class FilterPairs(dict):
    def __init__(self, *arg, **kw):
        super(FilterPairs, self).__init__(*arg, **kw)

    def _nested_set(self, keys, value):
        for key in keys[:-1]:
            self = self.setdefault(key, {})
        self[keys[-1]] = value

    def nested_set(self, keys, value, append=False):
        if append:
            try:
                val = self[keys[0]][keys[1]][keys[2]]
                for v in value:
                    val.append(v)
                self._nested_set(keys, val)
            except (KeyError):
                self._nested_set(keys, value)
        else:
            self._nested_set(keys, value)

    def __contains__(self, item):
        try:
            if item[0] in ["Name", "Event", "Place", "GPS"]:
                in_list = self[item[0]][item[1]][item[2]] == []
            else:
                in_list = item[3] in self[item[0]][item[1]][item[2]]
            return in_list
        except(KeyError):
            return False

class SessionParams:

    @staticmethod
    def _get_item_id(s):
        mi = re.search("(\S+) \((\d+)\)", s)
        if mi is not None:
            return int(s[mi.regs[2][0]:mi.regs[2][1]])
        else:
            return None

    @staticmethod
    def parse_line(line):
        p = line.split('\t')
        if len(p) < 4:
            sys.stderr.write("*Warning: Invalid line – ignored: " + line + "\n")
            return []
        tag = p[3]
        mi1 = SessionParams._get_item_id(p[0])
        mi2 = SessionParams._get_item_id(p[2])
        if mi1 is None or mi2 is None:
            sys.stderr.write("*Warning: No item IDs – ignored: " + line + "\n")
            return []

        if tag in ["Name", "Event", "Place", "GPS"]:
            return [tag, mi1, mi2, []]
        elif len(p) < 5:
            sys.stderr.write("*Ignored:" + line + "\n")
            return []

        values = p[4].split(", ")
        val_list = []
        for v in values:
            val_list.append(v[1:-1])        # remove quotes
        return [tag, mi1, mi2, val_list]

    @staticmethod
    def read_pairs(filename):
        pairs = FilterPairs()
        if filename is not None:
            if os.path.isfile(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    l = f.readline()
                    while l != "":
                        l = l.rstrip()
                        if l != "":
                            p = SessionParams.parse_line(l)
                            if p != []:
                                pairs.nested_set(p[0:3], p[3], append=True)
                        l = f.readline()
            else:
                sys.stderr.write(filename + " doesn't exist. Option -a ignored.\n")

        return pairs

    def __init__(self, tag_cat_list=[], fullpath=False, print_id=False, group=False, comp_name=None,
                 only_tags=False, tagvaluefile=None, filter_pairs=None, outfile=sys.stdout):
        self.fullpath = fullpath
        self.print_id = print_id
        self.group = group
        self.comp_name = comp_name
        self.tag_cat_list = tag_cat_list
        self.filter_list = FilterTags(tagvaluefile, only_tags)
        self.filter_pairs = self.read_pairs(filter_pairs)
        self.outfile = outfile

def main():
    global VerboseOutput

    alltags = ["Event", "Place", "GPS", "People", "Keywords", "Categories"]

    parser = argparse.ArgumentParser(
        description="Search inconcistent tags from a Daminion database.")

    # key identification arguments
    parser.add_argument("-g", "--group", dest="group", default=False,
                        action="store_true",
                        help="Use groups/stacks instead of image links")
    parser.add_argument("-f", "--fullpath", dest="fullpath", default=False,
                        action="store_true",
                        help="Print full directory path and not just file name")
    parser.add_argument("-i", "--id", dest="id", default=False,
                        action="store_true",
                        help="Print database id after the filename")
    parser.add_argument("-t", "--tags", dest="taglist", nargs='*', default=alltags, choices=alltags,
                        help="Tag categories to be checked [all]. "
                        "Allowed values for taglist are Event, Place, GPS, People, Keywords and Categories.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-x", "--exclude", dest="exfile",
                        help="Configuration file for tag values that are excluded from comparison.")
    group.add_argument("-y", "--only", dest="onlyfile",
                        help="Configuration file for tag values that are only used for comparison.")
    parser.add_argument("-a", "--acknowledged", dest="ack_pairs",
                       help="File containing list of acknowledged differences.")
    parser.add_argument("-v", "--verbose", action="count", dest="verbose", default=0,
                        help="verbose output (always into stdout)")
    parser.add_argument("-b", "--basename", dest="basename", nargs='*', metavar="SEPARATOR",
                        help="Compare the basename of the files. If additional strings are specified, "
                             "those are also used as separators, unless the filename is <= 8 chars.")
    parser.add_argument("-l", "--sqlite", dest="sqlite", default=False,
                        action="store_true",
                        help="Use Sqlite (= standalone) instead of Postgresql (=server)")
    parser.add_argument("-c", "--catalog", dest="dbname", default="NetCatalog",
                        help="Daminion catalog name [NetCatalog]")
    parser.add_argument("-s", "--server", dest="server", default="localhost",
                        help="Postgres server [localhost]")
    parser.add_argument("-p", "--port", dest="port", type=int, default=5432,
                        help="Postgres server port [5432]")
    parser.add_argument("-u", "--user", dest="user", default="postgres/postgres",
                        help="Postgres user/password [postgres/postgres]")
    parser.add_argument("-o", "--output", dest="outfile", type=argparse.FileType('w', encoding='utf-8'),
                        default=sys.stdout, help="Output file for report [stdout]")
    parser.add_argument("--version",
                        action="store_true", dest="version", default=False,
                        help="Display version information and exit.")

    args = parser.parse_args()

    VerboseOutput = args.verbose
    if args.version or VerboseOutput > 0:
        print(__doc__)
        print('*** Version', __version__, '***')
        if not args.version:
            print('Scan started at {:%H:%M:%S}'.format(datetime.datetime.now()))
        else:
            sys.exit(0)

    user = args.user.split('/')[0]
    password = args.user.split('/')[1]
    catalog = DamCatalog(args.server, args.port, args.dbname, user, password, args.sqlite)

    # document the call parameters in the output file
    line = sys.argv[0]
    for s in sys.argv[1:]:
        line += ' ' + s
    line += '\n'
    args.outfile.write(line)

    if args.onlyfile is None:
        file = args.exfile
    else:
        file = args.onlyfile
    session = SessionParams(args.taglist, args.fullpath, args.id, args.group, args.basename,
                            args.onlyfile == file, file, args.ack_pairs, args.outfile)
    catalog.ScanCatalog(session)

    if session.outfile != sys.stdout:
        session.outfile.close()

    return 0


if __name__ == '__main__':
    main()
