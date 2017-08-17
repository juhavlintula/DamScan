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
import datetime
import argparse
from Daminion.SessionParams import SessionParams
from Daminion.DamCatalog import DamCatalog
from Daminion.DamImage import DamImage, get_image_by_name

__version__ = "0.1.0"
__doc__ = "This program is checking if all the linked or grouped items in a Daminion catalog have same tags."

#   Version history
#   0.1.0    – first released version

def compare_image(img1, img2, session):
    same, tags = img1.image_eq(img2)
    if img2 == None or not same:
        if img2 == None:
            name2 = "–"
        else:
            name2 = img2.ImageName
        session.outfile.write(img1.ImageName + "\t<>\t" + name2)
        first = True
        for t in tags:
            if first:
                session.outfile.write("\t")
                first = False
            else:
                session.outfile.write(", ")
            session.outfile.write(t)
        session.outfile.write("\n")

def ScanCatalog(catalog1, catalog2, session, verbose=0):
    session.outfile.write("ImageA\tDir\tImageB\tTags\n")
    taglist = session.tag_cat_list
    for curr_img in DamCatalog.NextImage(catalog1, session, verbose):
        if curr_img.isvalid:
            img2 = get_image_by_name(curr_img._ImagePath, curr_img._ImageName, catalog2, session)
            compare_image(curr_img, img2, session)

def main():
    alltags = ["Event", "Place", "GPS", "People", "Keywords", "Categories"]

    parser = argparse.ArgumentParser(
        description="Search inconcistent tags from a Daminion database.")

    # key identification arguments
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
    parser.add_argument("-l", "--sqlite", dest="sqlite", default=False,
                        action="store_true",
                        help="Use Sqlite (= standalone) instead of Postgresql (=server)")
    parser.add_argument("-c1", "--catalog1", dest="dbname1", default="NetCatalog",
                        help="Daminion catalog name [NetCatalog]")
    parser.add_argument("-c2", "--catalog2", dest="dbname2", default="NetCatalog",
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

    if args.dbname1 == args.dbname2:
        sys.stderr.write("dbname 1 ({}) is the same as dbname2\n".format(args.dbname1))
        sys.exit(-1)

    user = args.user.split('/')[0]
    password = args.user.split('/')[1]
    catalog1 = DamCatalog(args.server, args.port, args.dbname1, user, password, args.sqlite)
    catalog1.initCatalogConstants()
    if VerboseOutput > 0:
        print("Database", args.dbname1, "opened and datastructures initialized.")
    catalog2 = DamCatalog(args.server, args.port, args.dbname2, user, password, args.sqlite)
    catalog2.initCatalogConstants()
    if VerboseOutput > 0:
        print("Database", args.dbname2, "opened and datastructures initialized.")

    # document the call parameters in the output file
    line = sys.argv[0]
    for s in sys.argv[1:]:
        line += ' ' + s
    line += '\n'
    args.outfile.write(line)

    session = SessionParams(args.taglist, args.fullpath, args.id, False, None, False, None, None, args.outfile)
    #       For verbose print the filter list

    ScanCatalog(catalog1, catalog2, session, VerboseOutput)

    if session.outfile != sys.stdout:
        session.outfile.close()

    return 0


if __name__ == '__main__':
    main()
