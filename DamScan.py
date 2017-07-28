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

__version__ = "1.0.2"
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
            if include:
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

    def __getMultiValueTags(self, tag, table, listname):
        cur = self.__db.catalog.cursor()
        cur.execute("SELECT id_value FROM " + table + " WHERE id_mediaitem=" + str(self.__id) + " ORDER BY id_value")
        rows = cur.fetchall()
        setattr(self, tag, [])
        for r in rows:
            if r[0] in getattr(self.__db, listname):
                getattr(self, tag).append(getattr(self.__db, listname)[r[0]])
            else:
                getattr(self, tag).append("–ERROR–")
                sys.stderr.write("***ERROR: Invalid {} in id: {}, image: {}\\{}\n".format(
                        tag, self.__id, self._ImagePath, self.ImageName))
        cur.close()

    def __init__(self, db, img_id):

        #        imagefiletypes = ["jpeg", "jpg", "tif", "tiff", "cr2", "crw", "nef", "dng"]
        global imagefiletypekey

        self.__db = db
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
                sys.stderr.write("***ERROR: No corresponding file entry for {} (id: {})\n".format(
                    self._ImageName, img_id))
            cur.close()
            return

        self._ImageName = row[0]
        self._ImagePath = row[1]

        # get Event and Media format (id)
        cur.execute("SELECT id_event, id_mediaformat, deleted FROM mediaitems WHERE id=" + str(img_id))
        row = cur.fetchone()
        self.IsDeleted = bool(row[2])
        if row[0] in self.__db.EventList:
            self.Event = self.__db.EventList[row[0]]
        else:
            self.Event = "–ERROR–"
            sys.stderr.write("***ERROR: Invalid Event in id: {}, image: {}\\{}\n".format(
                self.__id, self._ImagePath, self.ImageName))
        if row[1] in self.__db.MediaList:
            self.IsImage = self.__db.MediaList[row[1]] in imagefiletypekey
        else:
            self.IsImage = False
            sys.stderr.write("***ERROR: Invalid Media format in id: {}, image: {}\\{}\n".format(
                self.__id, self._ImagePath, self.ImageName))

        # get Place
        cur.execute("SELECT id_value FROM place_file WHERE id_mediaitem=" + str(img_id))
        rows = cur.fetchall()
        placelist = []
        for r in rows:
            placelist.append(self.__db.PlaceList[r[0]])
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
        self.__getMultiValueTags("People", "people_file", "PeopleList")
        self.__getMultiValueTags("Keywords", "keywords_file", "KeywordList")
        self.__getMultiValueTags("Categories", "categories_file", "CategoryList")

    @property
    def ImageName(self):
        line = ""
        if self.__db.FullPath:
            line += self._ImagePath + "\\"
        line += self._ImageName
        if self.__db.PrintID:
            line += " ({})".format(self.__id)
        return line

    @property
    def basename(self):
        name = self._ImageName.rsplit('.', maxsplit=1)[0]
        if self.__db.CheckName is not None:
            for c in self.__db.CheckName:
                tmp = name.rsplit(c, maxsplit=1)[0]
                if len(tmp) < 8:
                    break
                name = tmp
        return name

    #  Ignore deleted entries and items that are not images
    @property
    def isvalid(self):
        return not self.IsDeleted and self.IsImage

    def __repr__(self):
        return self.ImageName

    def _linked(self, select, where):
        tmp_list = []
        cur = self.__db.catalog.cursor()
        cur.execute("SELECT " + select + " FROM mediaitems_link WHERE " + where + "=" + str(self.__id))
#        if VerboseOutput:
#            print("Linked {} count {}: {}".format(where, self.ImageName, cur.rowcount))
        row = cur.fetchall()
        for r in row:
            img = DamImage(self.__db, r[0])
            if img.isvalid:
                tmp_list.append(img)
        cur.close()
        return tmp_list

    def _topItem(self):
        cur = self.__db.catalog.cursor()
        cur.execute("SELECT id_topmediaitemstack FROM mediaitems WHERE id=" + str(self.__id))
        row = cur.fetchone()
        cur.close()
        if row[0] == self.__id or row[0] is None:
            return []
        else:
            img = DamImage(self.__db, row[0])
            if img.isvalid:
                return [img]
            else:
                return []

    def _bottomItems(self):
        tmp_list = []
        cur = self.__db.catalog.cursor()
        cur.execute("SELECT id FROM mediaitems WHERE id_topmediaitemstack=" + str(self.__id))
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            if r[0] != self.__id:
                img = DamImage(self.__db, r[0])
                if img.isvalid:
                    tmp_list.append(img)
        return tmp_list

    def LinkedTo(self):
        if self.__db.useGroup:
            return self._topItem()
        else:
            return self._linked("id_tomediaitem", "id_frommediaitem")

    def LinkedFrom(self):
        if self.__db.useGroup:
            return self._bottomItems()
        else:
            return self._linked("id_frommediaitem", "id_tomediaitem")

    def GetTags(self, tag):
        tags = getattr(self, tag)
        return tags

    def SameSingleValueTag(self, other, tagcat, filter_list):
        mytag = self.GetTags(tagcat)
        othertag = other.GetTags(tagcat)
        if filter_list.has_option(tagcat, mytag) or filter_list.has_option(tagcat, othertag):
            return

        line = self.ImageName + "\t<>\t" + other.ImageName + "\t" + tagcat
        orig_len = len(line)
        if mytag != othertag:
            line += "\t'" + mytag + "'\t<>\t'" + othertag + "'"
        if orig_len < len(line):
            self.__db.outfile.write(line + "\n")

    def SameMultiValueTags(self, d, other, tagcat, filter_list):
        mytags = self.GetTags(tagcat)
        othertags = other.GetTags(tagcat)
        line = self.ImageName + "\t" + d + "\t" + other.ImageName + "\t" + tagcat + "\t"
        orig_len = len(line)
        for tagvalue in othertags:
            if tagvalue not in mytags and not filter_list.has_option(tagcat, tagvalue) and tagvalue != "":
                if len(line) > orig_len:
                    line += ", "
                line += "'" + tagvalue + "'"
        if orig_len < len(line):
            self.__db.outfile.write(line + "\n")


class DamCatalog:
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
            getattr(self, listname)[rows[i][0]] = temp
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
        self.CheckName = None
        self.outfile = None
        self.catalog = None
        self.__dbname = name
        self.__counter = 0

        try:
            if sqlite:
                if os.path.isfile(name):
                    self.catalog = sqlite3.connect(name)
                else:
                    sys.stderr.write(name + " is not a valid database file\n")
                    sys.exit(-1)
            else:
                self.catalog = psycopg2.connect(host=host, port=port, database=name,
                                                user=user, password=pwd)

        except (Exception, psycopg2.DatabaseError) as error:
            sys.stderr.write(error + "\n")
            sys.exit(-1)

        self._initMediaList()
        self._initEventList()
        self._initPlaceList()
        self._initPeopleList()
        self._initKeywordList()
        self._initCategoryList()

        if VerboseOutput > 0:
            print("Database", self.__dbname, "opened and datastructures initialized.")

    def __del__(self):
        if self.catalog is not None:
            self.catalog.close()

    def __repr__(self):
        return "{}".format(self.__dbname)

    def set_params(self, group, basename, fullpath, id, outfile):
        self.useGroup = group
        self.FullPath = fullpath
        self.PrintID = id
        self.outfile = outfile
        self.CheckName = basename

    def NextImage(self):
        curs = self.catalog.cursor()
        curs.execute("SELECT id, filename, deleted FROM mediaitems")
#        if VerboseOutput > 0:
#            count = curs.rowcount
#            if count > 0:
#                print("Number of files: ", count)

        row = curs.fetchone()    # id, filename, deleted
        while row is not None:
            if not bool(row[2]):
                self.__counter += 1
                if VerboseOutput > 0:
                    print("\r", "{:7} {:7}: {:60}".format(self.__counter, row[0], row[1]), end="", flush=True)
                yield DamImage(self, row[0])
            row = curs.fetchone()

        curs.close()
        raise StopIteration

    def ScanCatalog(self, taglist, exclude):
        self.outfile.write("ImageA\tDir\tImageB\tTag\tValueA/Missing A\t\tValueB\n")
        for curr_img in self.NextImage():
            if not curr_img.isvalid:
                continue
            ToList = curr_img.LinkedTo()
            FromList = curr_img.LinkedFrom()
            if ToList == [] and FromList == []:
                continue
            if VerboseOutput > 1:
                print("")
                print("{}\t{}\t{}".format(FromList, curr_img, ToList))
            if self.CheckName is not None:
                for lst in ToList, FromList:
                    for f in lst:
                        if curr_img.basename != f.basename:
                            self.outfile.write(curr_img.ImageName + "\t<>\t" + f.ImageName + "\n")
            for tag in taglist:
                if tag in ["Event", "Place", "GPS"]:    # single value tags
                    for img in ToList:
                        curr_img.SameSingleValueTag(img, tag, exclude)
                else:                                   # multi value tags
                    for img in ToList:
                        curr_img.SameMultiValueTags(">", img, tag, exclude)
                    for img in FromList:
                        curr_img.SameMultiValueTags("<", img, tag, exclude)


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
    catalog.set_params(args.group, args.basename, args.fullpath, args.id, args.outfile)

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
    filter_list = FilterTags(file, args.onlyfile == file)
    catalog.ScanCatalog(args.taglist, filter_list)

    if catalog.outfile != sys.stdout:
        catalog.outfile.close()

    return 0


if __name__ == '__main__':
    main()
