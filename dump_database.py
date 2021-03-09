import os
import pathlib
import sqlite3
import sys


if __name__ == '__main__':
    ro_uri = r'%s?mode=ro' % pathlib.Path(
        os.path.abspath(sys.argv[1])).as_uri()
    connection = sqlite3.connect(ro_uri, uri=True)
    cursor = connection.execute('SELECT * FROM work_status;')
    payload = (cursor.fetchall())
    print(payload)
