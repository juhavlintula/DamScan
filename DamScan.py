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

__version__ = "1.1.1"
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
#   1.1.0   – new public release of the -a option
#   1.1.1   – refactored everething into multiple modules


def ScanCatalog(catalog, session, verbose=0):
    session.outfile.write("ImageA\tDir\tImageB\tTag\tValueA/Missing A\t\tValueB\n")
    taglist = session.tag_cat_list
    exclude = session.filter_list
    for curr_img in DamCatalog.NextImage(catalog, session, verbose):
        if not curr_img.isvalid:
            continue
        if session.group:  # by groups
            ToList = curr_img.top_item()
            FromList = curr_img.bottom_items()
        else:  # by links
            ToList = curr_img.linked("id_tomediaitem", "id_frommediaitem")
            FromList = curr_img.linked("id_frommediaitem", "id_tomediaitem")
        if ToList == [] and FromList == []:
            continue
        if verbose > 1:
            print("\n{}\t{}\t{}".format(FromList, curr_img, ToList))
        if session.comp_name is not None:
            for lst in ToList, FromList:
                for f in lst:
                    if curr_img.basename != f.basename and ("Name", curr_img._id, f._id) not in session.filter_pairs:
                        session.outfile.write(curr_img.ImageName + "\t<>\t" + f.ImageName + "\tName\n")
        for tag in taglist:
            if tag in ["Event", "Place", "GPS"]:  # single value tags
                for img in ToList:
                    curr_img.SameSingleValueTag(img, tag, exclude, session.filter_pairs)
            else:  # multi value tags
                for img in ToList:
                    curr_img.SameMultiValueTags(">", img, tag, exclude, session.filter_pairs)
                for img in FromList:
                    curr_img.SameMultiValueTags("<", img, tag, exclude, session.filter_pairs)


def main():
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
    catalog.initCatalogConstants()
    if VerboseOutput > 0:
        print("Database", args.dbname, "opened and datastructures initialized.")

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
    #       For verbose print the filter list
    if VerboseOutput > 0:
        line = "Tags that are"
        if args.onlyfile == file:
            line += " not"
        line += " filtered are:"
        print(line)
        for s in session.filter_list.sections():
            print("[{}]".format(s))
            for o in session.filter_list.options(s):
                print(o)
        print("")

    ScanCatalog(catalog, session, VerboseOutput)

    if session.outfile != sys.stdout:
        session.outfile.close()

    return 0


if __name__ == '__main__':
    main()
