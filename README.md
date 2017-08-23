# DamScan and DamCompare
A Python utility to identify inconsistencies between linked or grouped items (DamScan) in Daminion digital asset management (DAM) system and comparing the metadata in Daminion database and in the item itself (DamCompare).

Daminion digital asset management (DAM) system is a great tool for assigning meta data (tags) to your digital assets and for sorting and searching the items. In Daminion it’s also possible to link or group associated items together, but there are no built-in tools for checking the consistency of the meta data of these linked or grouped items. DamScan.py solves this problem and reports inconsistencies in meta data for Daminion server and standalone catalogs.

Daminion writes all tags (with few exceptions) into the media files, so that the metadata is also available outside Daminion. In Daminion there is no facility to verify if the metadata in the media items is the same as is in the Daminion database. DamCompare.py solves this problem by reporting inconsistencies in metadata for Daminion server and standalone catalogs. Inconsistencies can arise either by changing metadata in image file outside of Daminion, so the Daminion catalog is not aware of those changes or when changes in Daminion are not completely written into the metadata in the files.

You need to have Python 3.x and psycopg2 installed. See detailed installation instructions in manual pages.
