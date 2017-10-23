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
#   23Oct2017: Suggested addition to calculate distance in meters between two GPS coordinates, marked with ->   # WBL

import sys
import sqlite3
import psycopg2
import psycopg2.extensions

from math import cos, asin, sqrt                                                                                # WBL

imagefiletypekey = ["%7jnbapuim4$lwk:d45bb3b6-b441-435c-a3ec-b27d067b7c53",
                    "%7jnbapuim4$lwk:343f9214-79a7-4b58-96a3-b7838e3e37ee"]  # magic keys from database


def get_image_by_name(path, name, db, session):
    # this should do cur.fetchall() and select the row which is not deleted
    # no this is taking the first row from the database
    cur = db.catalog.cursor()
    if isinstance(cur, sqlite3.Cursor):
        cur.execute("SELECT id_mediaitem FROM files WHERE filename = ? AND relativepath = ?", (name, path))
    elif isinstance(cur, psycopg2.extensions.cursor):
        cur.execute("SELECT id_mediaitem FROM files WHERE filename = %s AND relativepath = %s", (name, path))
    row = cur.fetchall()
    if row is None: # or row[0] is None:
        return None
    else:
        for r in row:
            if isinstance(cur, sqlite3.Cursor):
                cur.execute("SELECT deleted FROM mediaitems WHERE id=?", (r[0], ))
            elif isinstance(cur, psycopg2.extensions.cursor):
                cur.execute("SELECT deleted FROM mediaitems WHERE id=%s", (r[0], ))
            d = cur.fetchone()
            if d is not None and not bool(d[0]):
                return DamImage(r[0], db, session)
        return None


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

        cur.execute("SELECT deleted, id_event, id_mediaformat, creationdatetime FROM mediaitems WHERE id=" +
                    str(img_id))
        row = cur.fetchone()
        isdeleted = row is None or bool(row[0])
        if isdeleted:
            return "", False, isdeleted, None
        if row[1] in eventlist:
            event = eventlist[row[1]]
        else:
            event = "–ERROR–"
            sys.stderr.write("***ERROR: Invalid Event in id: {}, image: {}\n".format(img_id, filename))
        if row[2] in medialist:
            isimage = medialist[row[2]] in imagefiletypekey
        else:
            isimage = False
            sys.stderr.write("***ERROR: Invalid Media format in id: {}, image: {}\n".format(img_id, filename))
        return event, isimage, isdeleted, row[3]

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
        return GPSstring, lat, long, alt   # Return coordinates for later calculation                           # WBL


    @staticmethod
    def _none_to_str(s):
        if s is None:
            return ""
        else:
            return s

    @staticmethod
    def _get_subject(cur, img_id):
        cur.execute("SELECT title, description, comments FROM subject WHERE id_mediaitem=" + str(img_id))
        row = cur.fetchone()
        if row is None:
            title = ""
            description = ""
            comments = ""
        else:
            title = DamImage._none_to_str(row[0])
            description = DamImage._none_to_str(row[1])
            comments = DamImage._none_to_str(row[2])
        return title, description, comments

    def __init__(self, img_id, db, session):
        self._db = db
        self._id = img_id
        self._session = session

        cur = self._db.catalog.cursor()
        self._ImageName, self._ImagePath, err_flag = self._get_filename(cur, self._id)
        if err_flag:
            self.IsDeleted = True
            self.IsImage = False
        filename = self._ImagePath + "\\" + self._ImageName
        self.Event, self.IsImage, self.IsDeleted, self.creationtime = self._get_mediaitems_attr(cur, self._id, filename,
                                                                             self._db.MediaList, self._db.EventList)
        if self.IsDeleted:
            self.Place = ""
            self.GPS = ""
            self.People = []
            self.Keywords =[]
            self.Categories = []
            self.Collections = []
            self.Title = ""
            self.Description = ""
            self.Comments = ""
        else:
            self.Place = self._get_place(cur, self._id, filename, self._db.PlaceList)
            self.GPS, self.lat, self.long, self.alt = self._get_GPS(cur, self._id)                              # WBL
            self.Title, self.Description, self.Comments = self._get_subject(cur, self._id)

            # get list of People, Keywords and Categories
            self.People = self._getMultiValueTags(cur, self._id, "People", "people_file", filename, self._db.PeopleList)
            self.Keywords = self._getMultiValueTags(cur, self._id, "Keywords", "keywords_file", filename,
                                                    self._db.KeywordList)
            self.Categories = self._getMultiValueTags(cur, self._id, "Categories", "categories_file", filename,
                                                      self._db.CategoryList)
            self.Collections = self._getMultiValueTags(cur, self._id, "Collections", "systemcollection_file", filename,
                                                       self._db.CollectionList)
        cur.close()

    def image_eq(self, other):
        if other is None or other.IsDeleted:
            return False, ["ERROR: file missing"]
        lst = []
#        if self._ImageName != other._ImageName:
#            lst.append("Filename")
#        if self._ImagePath != other._ImagePath:
#            lst.append("Path")
        if self.creationtime != other.creationtime:
            lst.append("Creation Time")
        if self.Title != other.Title:
            lst.append("Title")
        if self.Description != other.Description:
            lst.append("Description")
        if self.Comments != other.Comments:
            lst.append("Comments")
        if self.Place != other.Place:
            lst.append("Place")
        if self.GPS != other.GPS:

            other_lat  = float(other.lat)                                                  # WBL    Calculate
            other_long = float(other.long)                                                 # WBL    distance in mtr
            other_alt  = float(other.alt)                                                  # WBL    between
            delta_lat  = other_lat  - self.lat                                             # WBL    two
            delta_long = other_long - self.long                                            # WBL    GPS coordinates
            delta_alt  = other_alt  - self.alt                                             # WBL

# Calculate the distance of two points defined by latitude and longitude
# Formular taken from:
# https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
            p = 0.017453292519943295  # Pi/180                                              # WBL
            a = 0.5 - cos(delta_lat*p) / 2 + cos(self.lat*p) * cos(other_lat*p) * (1 - cos(delta_long*p))/2  # WBL
            distance = 12.742 * asin(sqrt(a))           # distance in meter!                # WBL

            if distance > 1.0 or delta_alt > 0.0:       # report only when distance > 1 m   # WBL
                lst.append("GPS")                       # or different altitude >0 m        # WBL
                lst.append(str(distance)+" m distance" )                                    # WBL
                if delta_alt > 0:                                                           # WBL
                    lst.append(str(delta_alt)+" m elev. difference")                        # WBL

        if self.Event != other.Event:
            lst.append("Event")
        if sorted(self.People) != sorted(other.People):
            lst.append("People")
        if sorted(self.Keywords) != sorted(other.Keywords):
            lst.append("Keywords")
        if sorted(self.Categories) != sorted(other.Categories):
            lst.append("Categories")
        if sorted(self.Collections) != sorted(other.Collections):
            lst.append("Collections")
        return lst == [], lst

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
            if r[0] != self._id:
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
        if row is None or row[0] == self._id:
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
            line = line.replace("\r", "")
            line = line.replace("\n", "\xB6")
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
