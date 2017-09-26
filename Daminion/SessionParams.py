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
import re
import configparser


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
            if item[0] in ["Name", "Event", "Place", "GPS", "Title", "Description", "Comments"]:
                in_list = self[item[0]][item[1]][item[2]] == []
            else:
                in_list = item[3] in self[item[0]][item[1]][item[2]]
            return in_list
        except(KeyError):
            return False


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
        configparser.ConfigParser.__init__(self, allow_no_value=True, delimiters=('\n',))
        self.optionxform = str
#        self.delimiters = ('■',)
        self.has_option = self._has_option
        if filterfile is not None:
            if self.read(filterfile, encoding='utf-8') == []:
                sys.stderr.write("File " + filterfile + " specified with -x|-y doesn't exist. Option ignored.\n")
            elif include:
                self.has_option = self._has_no_option


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
                 only_tags=False, tagvaluefile=None, filter_pairs=None, exdir=[], onlydir=[], outfile=sys.stdout):
        self.fullpath = fullpath
        self.print_id = print_id
        self.group = group
        self.comp_name = comp_name
        self.tag_cat_list = tag_cat_list
        self.filter_list = FilterTags(tagvaluefile, only_tags)
        self.filter_pairs = self.read_pairs(filter_pairs)
        if exdir is None:
            self.exdir = []
        else:
            self.exdir = exdir
        if onlydir is None:
            self.onlydir = []
        else:
            self.onlydir = onlydir
        self.outfile = outfile
