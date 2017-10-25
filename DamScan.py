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
import configparser
from Daminion.SessionParams import SessionParams
from Daminion.DamCatalog import DamCatalog

__version__ = "1.5.0"
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
#   1.2.0   – added support for INI file
#           – added support for Collections
#   1.2.1   – fix for an empty Outfile parameter
#   1.3.0   – some updates to INI file structure
#   1.4.0   – added Title, Description and Comments
#   1.5.0   - added support to GPS precision (based on Wilfried's changes

alltags = ["Event", "Place", "GPS", "Title", "Description", "Comments", "People", "Keywords", "Categories",
           "Collections"]

def check_conf(conf):
    valid_config = {'Database': { 'sqlite': None, 'catalog': None, 'port': None, 'server': None, 'user': None },
                  'Session': { 'fullpath': None, 'id': None, 'group': None, 'basename': None, 'tags': None,
                               'acknowledged': None, 'excludetags': None, 'onlytags': None,
                               'gps_dist': None, 'gps_alt': None, 'outfile': None, 'verbose': None,
                               'exclude': None, 'only': None }}

    valid_conf = configparser.ConfigParser(allow_no_value=True)
    valid_conf.read_dict(valid_config)
    for sect in conf.sections():
        for key in conf.options(sect):
            if not valid_conf.has_option(sect,key):
                sys.stderr.write("* Warning: INI file has an invalid option '{}':'{}' – Ignored\n".format(sect, key))

def verifytags(taglist):
    verified = []
    for t in taglist:
        if t in alltags:
            verified.append(t)
        else:
            sys.stderr.write("* Warning: INI file has an invalid tag category '{}' – Ignored\n".format(t))
    return verified

def read_ini(args, conf):
    global alltags

    if args.ini_file is not None and conf.read(args.ini_file, encoding='utf-8') == []:
        sys.stderr.write("INI File " + args.ini_file + " doesn't exist. Option ignored.\n")
    check_conf(conf)

    if args.sqlite is None:
        args.sqlite = conf.getboolean('Database', 'SQLite', fallback=False)
    if args.dbname is None:
        if conf.has_option('Database', 'Catalog'):
            args.dbname = conf['Database']['Catalog']
        elif args.sqlite:
            args.dbname = "DaminionCatalog.dmc"
        else:
            args.dbname = "NetCatalog"
    if args.port is None:
        args.port = conf.getint('Database', 'Port', fallback=5432)
    if args.server is None:
        args.server = conf.get('Database', 'Server', fallback='localhost')
    if args.user is None:
        args.user = conf.get('Database', 'User', fallback="postgres/postgres")

    if args.fullpath is None:
        args.fullpath = conf.getboolean('Session', 'Fullpath', fallback=False)
    if args.id is None:
        args.id = conf.getboolean('Session', 'ID', fallback=False)
    if args.group is None:
        args.group = conf.getboolean('Session', 'Group', fallback=False)

    if args.basename is None:
        base = conf.get('Session', 'Basename', fallback=None)
        if base is not None:
            args.basename = base.split()
    if args.taglist is None:
        taglist = conf.get('Session', 'Tags', fallback=None)
        if taglist is None or taglist.lower()=="all":
            args.taglist = alltags
        else:
            args.taglist = taglist.split()
            args.taglist = verifytags(args.taglist)
    if args.ack_pairs is None:
        args.ack_pairs = conf.get('Session', 'acknowledged', fallback=None)
    if args.exfile is None and args.onlyfile is None:
        excf = conf.get('Session', 'excludetags', fallback=None)
        if excf is None:        # compatibility with old specs
            excf = conf.get('Session', 'exclude', fallback=None)
            if excf is not None:
                sys.stderr.write("* Warning: 'Exclude' is discontinued, use 'ExcludeTags' instead.\n")
        incf = conf.get('Session', 'onlytags', fallback=None)
        if incf is None:        # compatibility with old specs
            incf = conf.get('Session', 'only', fallback=None)
            if incf is not None:
                sys.stderr.write("* Warning: 'Only' is discontinued, use 'OnlyTags' instead.\n")
        if excf is not None and incf is not None:
            sys.stderr.write("* Warning: INI file has specified both excludetags={} and onlytags={} – {} ignored\n".format(
                excf, incf, incf))
            incf = None
        args.exfile = excf
        args.onlyfile = incf

    if args.dist_tolerance is None:
        args.dist_tolerance = conf.getfloat('Session', 'GPS_dist', fallback=0.0)
    if args.alt_tolerance is None:
        args.alt_tolerance = conf.getfloat('Session', 'GPS_alt', fallback=0.0)

    if args.outfilename is None:
        file = conf.get('Session', 'Outfile', fallback=None)
    else:
        file = args.outfilename
    if file is None or file == "" or file == "<stdout>":
        args.outfile = sys.stdout
    else:
        args.outfile = open(file, 'w', encoding='utf-8')
    if args.verbose is None:
        args.verbose = conf.getint('Session', 'Verbose', fallback=0)

def create_parser():
    global alltags

    parser = argparse.ArgumentParser(
        description="Search inconcistent tags from a Daminion database.")

    # key identification arguments
    parser.add_argument("--ini", dest="ini_file",
                       help="INI file for scan parameters.")
    parser.add_argument("-g", "--group", dest="group", #default=False,
                        action="store_const", const=True, default=None,
                        help="Use groups/stacks instead of image links")
    parser.add_argument("-f", "--fullpath", dest="fullpath", #default=False,
                        action="store_const", const=True, default=None,
                        help="Print full directory path and not just file name")
    parser.add_argument("-i", "--id", dest="id", #default=False,
                        action="store_const", const=True, default=None,
                        help="Print database id after the filename")
    parser.add_argument("-t", "--tags", dest="taglist", nargs='*', choices=alltags, #default=alltags,
                        help="Tag categories to be checked [all]. "
                        "Allowed values for taglist are Event, Place, GPS, Title, Description, Comments, People, "
                        "Keywords, Categories and Collections.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-x", "--excludetags","--exclude", dest="exfile",
                        help="Configuration file for tag values that are excluded from comparison.")
    group.add_argument("-y", "--onlytags", "--only", dest="onlyfile",
                        help="Configuration file for tag values that are only used for comparison.")
    parser.add_argument("-a", "--acknowledged", dest="ack_pairs",
                       help="File containing list of acknowledged differences.")
    parser.add_argument("--GPS_dist", dest="dist_tolerance", type=float, default=None,
                        help="Allowed GPS distance tolerance")
    parser.add_argument("--GPS_alt", dest="alt_tolerance", type=float, default=None,
                        help="Allowed GPS height tolerance")
    parser.add_argument("-v", "--verbose", action="count", dest="verbose", #default=0,
                        help="verbose output (always into stdout)")
    parser.add_argument("-b", "--basename", dest="basename", nargs='*', metavar="SEPARATOR",
                        help="Compare the basename of the files. If additional strings are specified, "
                             "those are also used as separators, unless the filename is <= 8 chars.")
    parser.add_argument("-l", "--sqlite", dest="sqlite", #default=False,
                        action="store_const", const=True, default=None,
                        help="Use Sqlite (= standalone) instead of Postgresql (=server)")
    parser.add_argument("-c", "--catalog", dest="dbname", #default="NetCatalog",
                        help="Daminion catalog name [NetCatalog]")
    parser.add_argument("-s", "--server", dest="server", #default="localhost",
                        help="Postgres server [localhost]")
    parser.add_argument("-p", "--port", dest="port", type=int, #default=5432,
                        help="Postgres server port [5432]")
    parser.add_argument("-u", "--user", dest="user", #default="postgres/postgres",
                        help="Postgres user/password [postgres/postgres]")
    parser.add_argument("-o", "--output", dest="outfilename", # type=argparse.FileType('w', encoding='utf-8'),
                        # default=sys.stdout,
                        help="Output file for report [stdout]")
    parser.add_argument("--version",
                        action="store_true", dest="version", default=False,
                        help="Display version information and exit.")

    conf = configparser.ConfigParser(allow_no_value=True)
    conf.BOOLEAN_STATES['include'] = True
    conf.BOOLEAN_STATES['exclude'] = False
#   conf.delimeters = ('=', )

    return parser, conf


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
            if tag in ["Event", "Place", "GPS", "Title", "Description", "Comments"]:  # single value tags
                for img in ToList:
                    curr_img.SameSingleValueTag(img, tag, exclude, session.filter_pairs, session.dist_tolerance,
                                                session.alt_tolerance)
            else:  # multi value tags
                for img in ToList:
                    curr_img.SameMultiValueTags(">", img, tag, exclude, session.filter_pairs)
                for img in FromList:
                    curr_img.SameMultiValueTags("<", img, tag, exclude, session.filter_pairs)


def main():
    parser, conf = create_parser()
    args = parser.parse_args()
    read_ini(args, conf)

    VerboseOutput = args.verbose
    if args.version or VerboseOutput > 0:
        print(__doc__)
        print(sys.argv[0], ' *** Version', __version__, '***')
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
                            args.onlyfile == file, file, args.ack_pairs, args.dist_tolerance, args.alt_tolerance,
                            outfile=args.outfile)
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
