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
import shlex
from Daminion.SessionParams import SessionParams
from Daminion.DamCatalog import DamCatalog
from Daminion.DamImage import DamImage, get_image_by_name

__version__ = "1.5.2"
__doc__ = "This program compares metadata of items in two Daminion catalogs."

#   Version history
#   0.1.0   – first released version
#   0.2.0   – updated the options, added creation date
#   0.3.0   – added collections
#   0.4.0   – bug fixes and added -y option to path for images to be compared
#   0.4.1   – improved handling of deleted items in get_image_by_name
#   0.4.2   – updated program desrciption message
#   0.5.0   – added support for INI file
#   1.2.1   – sync the version numbering with DamScan.py
#   1.3.0   – added support for exclude folders
#   1.4.0   – added Title, Description and Comments
#   1.5.0   - added support to GPS precision (based on Wilfried's changes
#   1.5.1   - ignore milliseconds in creation time comparison
#   1.5.2   - fixed the different datetime representation in SQLite

alltags = ["Event", "Place", "GPS", "Title", "Description", "Comments", "People", "Keywords", "Categories",
           "Collections"]


def check_conf(conf):
    valid_config = {'Database': { 'sqlite': None, 'catalog1': None, 'catalog2': None, 'port': None, 'server': None,
                                  'user': None },
                  'Session': { 'fullpath': None, 'id': None, 'excludepaths': None, 'onlypaths': None,'outfile': None,
                               'gps_dist': None, 'gps_alt': None, 'verbose': None, 'exclude': None, 'only': None}}

    valid_conf = configparser.ConfigParser(allow_no_value=True)
    valid_conf.read_dict(valid_config)
    for sect in conf.sections():
        for key in conf.options(sect):
            if not valid_conf.has_option(sect,key):
                sys.stderr.write("* Warning: INI file has an invalid option '{}':'{}' – Ignored\n".format(sect, key))

def read_ini(args, conf):
    global alltags

    if args.ini_file is not None and conf.read(args.ini_file, encoding='utf-8') == []:
        sys.stderr.write("INI File " + args.ini_file + " doesn't exist. Option ignored.\n")
    check_conf(conf)

    if args.sqlite is None:
        args.sqlite = conf.getboolean('Database', 'SQLite', fallback=False)
    if args.dbname1 is None:
        args.dbname1 = conf.get('Database', 'Catalog1', fallback=None)
    if args.dbname2 is None:
        args.dbname2 = conf.get('Database', 'Catalog2', fallback=None)
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

    if args.dist_tolerance is None:
        args.dist_tolerance = conf.getfloat('Session', 'GPS_dist', fallback=0.0)
    if args.alt_tolerance is None:
        args.alt_tolerance = conf.getfloat('Session', 'GPS_alt', fallback=0.0)

    #   if args.taglist is None:
#       taglist = conf.get('Session', 'Tags', fallback=None)
#       if taglist is None or taglist.lower()=="all":
#           args.taglist = alltags
#       else:
#           args.taglist = taglist.split()
    no_cmd_ex = args.exdir is None
    no_cmd_inc = args.onlydir is None
    if args.exdir is None:
        excf = conf.get('Session', 'excludepaths', fallback="")
        if excf == "":
            excf = conf.get('Session', 'exclude', fallback="")
            if excf != "":
                sys.stderr.write("* Warning: 'Exclude' is discontinued, use 'ExcludePaths' instead.\n")
        args.exdir = shlex.split(excf)
    if args.onlydir is None:
        incf = conf.get('Session', 'onlypaths', fallback="")
        if incf == "":
            incf = conf.get('Session', 'only', fallback="")
            if incf != "":
                sys.stderr.write("* Warning: 'Only' is discontinued, use 'OnlyPaths' instead.\n")
        args.onlydir = shlex.split(incf)
    if args.onlydir != [] and args.exdir != []:
        if no_cmd_ex and no_cmd_inc:
            sys.stderr.write("* Warning: INI file has specified both excludepaths={} and onlypaths={} – {} ignored\n".format(
                args.exdir, args.onlydir, args.onlydir))
            args.onlydir = []
        elif no_cmd_ex:
            args.exdir = []
        elif no_cmd_inc:
            args.onlydir = []

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
    parser.add_argument("-f", "--fullpath", dest="fullpath", #default=False,
                        action="store_const", const=True, default=None,
                        help="Print full directory path and not just file name")
    parser.add_argument("-i", "--id", dest="id", #default=False,
                        action="store_const", const=True, default=None,
                        help="Print database id after the filename")
    #   parser.add_argument("-t", "--tags", dest="taglist", nargs='*', choices=alltags, #default=alltags,
    #                    help="Tag categories to be checked [all]. "
    #                    "Allowed values for taglist are Event, Place, GPS, People, Keywords, Categories and Collections.")
    parser.add_argument("--GPS_dist", dest="dist_tolerance", type=float, default=None,
                        help="Allowed GPS distance tolerance")
    parser.add_argument("--GPS_alt", dest="alt_tolerance", type=float, default=None,
                        help="Allowed GPS height tolerance")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-x", "--excludepaths", "--exclude", dest="exdir", nargs='+',
                        help="List of folder paths that are excluded from comparison.")
    group.add_argument("-y", "--onlypaths", "--only", dest="onlydir", nargs='+',
                        help="List of folder paths that are included for comparison.")
    group = parser.add_mutually_exclusive_group()

    parser.add_argument("-l", "--sqlite", dest="sqlite", #default=False,
                        action="store_const", const=True, default=None,
                        help="Use Sqlite (= standalone) instead of Postgresql (=server)")
    parser.add_argument("-c1", "--catalog1", dest="dbname1", # nargs=1,  # default="NetCatalog",
                        help="Daminion catalog name [NetCatalog]")
    parser.add_argument("-c2", "--catalog2", dest="dbname2", # nargs=1,  # default="NetCatalog",
                        help="Daminion catalog name [NetCatalog]")
    parser.add_argument("-s", "--server", dest="server", #default="localhost",
                        help="Postgres server [localhost]")
    parser.add_argument("-p", "--port", dest="port", type=int, #default=5432,
                        help="Postgres server port [5432]")
    parser.add_argument("-u", "--user", dest="user", #default="postgres/postgres",
                        help="Postgres user/password [postgres/postgres]")

    parser.add_argument("-v", "--verbose", action="count", dest="verbose", #default=0,
                        help="verbose output (always into stdout)")
    parser.add_argument("-o", "--output", dest="outfilename", # type=argparse.FileType('w', encoding='utf-8'),
                        # default=sys.stdout,
                        help="Output file for report [stdout]")
    parser.add_argument("--version",
                        action="store_true", dest="version", default=False,
                        help="Display version information and exit.")

    conf = configparser.ConfigParser(allow_no_value=True)

    return parser, conf

def compare_image(img1, img2, session):
    same, tags = img1.image_eq(img2, session.dist_tolerance, session.alt_tolerance)
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

def valid_path(path, session):
    if session.exdir == [] and session.onlydir == []:
        return True
    if session.onlydir != []:
        dirlist = session.onlydir
    elif session.exdir != []:
        dirlist = session.exdir
    match = False
    for d in dirlist:
        match |= d == path[:len(d)]
        if match:
            break
    return (match and session.onlydir != []) or (not match and session.exdir != [])

def ScanCatalog(catalog1, catalog2, session, verbose=0):
    session.outfile.write("{}\tDir\t{}\tTags\n".format(catalog1._dbname, catalog2._dbname))
    taglist = session.tag_cat_list
    for curr_img in DamCatalog.NextImage(catalog1, session, verbose):
        if curr_img.isvalid:
            comp = valid_path(curr_img._ImagePath, session)
            if comp:
                img2 = get_image_by_name(curr_img._ImagePath, curr_img._ImageName, catalog2, session)
                compare_image(curr_img, img2, session)

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

    if args.dbname1 is None or args.dbname2 is None:
        sys.stderr.write("dbname 1 ({}) and/or dbname2 ({}) cannot be empty.\n".format(args.dbname1, args.dbname2))
        sys.exit(-1)
    if args.dbname1 == args.dbname2:
        sys.stderr.write("dbname 1 ({}) is the same as dbname2\n".format(args.dbname1[0]))
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

    session = SessionParams(None, args.fullpath, args.id,
                            dist_tolerance=args.dist_tolerance, alt_tolerance=args.alt_tolerance,
                            exdir=args.exdir, onlydir=args.onlydir,
                            outfile=args.outfile)

    ScanCatalog(catalog1, catalog2, session, VerboseOutput)

    if session.outfile != sys.stdout:
        session.outfile.close()

    return 0


if __name__ == '__main__':
    main()
