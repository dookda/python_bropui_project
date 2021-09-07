import os
import pandas as pd
import numpy as np
import ftplib
import csv
import psycopg2 as pg2
import re

conn = pg2.connect(user="postgres", password="Pgis@rti2dss@2020",
                   host="119.59.125.134", database="envidb")
cur = conn.cursor()

# connect ftp
ftphost = '203.154.140.35'
ftp = ftplib.FTP(ftphost, 'Etc', 'Paris1234')
ftpdir = '/UM6972/CSV/'
ftp.cwd(ftpdir)
files = ftp.nlst()

# check stock
cur.execute("SELECT filename FROM vibrate")
rows = cur.fetchall()
savedfile = []
for r in rows:
    savedfile.append(r[0])
print(f"savedfile {savedfile}")


def convNumber(x):
    x = x.replace("<", "")
    x = x.replace(">", "")
    isNumber = any(map(str.isdigit, x))
    z = isNumber and x or 0
    # print(z)
    return z


def insertData(fpath, fname):
    dat = {}
    with open(fpath) as fd:
        reader = csv.reader(fd)
        rows = [r for r in reader]

        for r in rows[:58]:
            x = r[1].split(" ")
            # isNumber = any(map(str.isdigit, x[0]))
            # print(type(x[0]), x[0])
            # xNumber = isNumber and x[0] or 0
            dat[r[0].lower()] = x[0]
    # print(dat)
    sql = f'''INSERT INTO vibrate(
            ts, filename, 
            tranppv,
            vertppv,
            longppv,
            tranppvponderated,
            vertppvponderated,
            longppvponderated,
            tranppvdb,
            vertppvdb,
            longppvdb
        )VALUES(
            '{dat["eventdate"]} {dat["eventtime"]}', '{fname}', 
            {convNumber(dat["tranppv"])},
            {convNumber(dat["vertppv"])},
            {convNumber(dat["longppv"])},
            {convNumber(dat["tranppvponderated"])},
            {convNumber(dat["vertppvponderated"])},
            {convNumber(dat["longppvponderated"])},
            {convNumber(dat["tranppvdb"])},
            {convNumber(dat["vertppvdb"])},
            {convNumber(dat["longppvdb"])}
        )'''
    # print(sql)

    cur.execute(sql)
    conn.commit()


def loadCSV(fname):
    fpath = r'/Users/sakdahomhuan/Dev/python_bropui_project/csv/'+fname
    ftp.retrbinary("RETR "+fname, open(fpath, 'wb').write)
    print(fname)
    insertData(fpath, fname)
    os.remove(fpath)


# retrieve file
def initLoadLastfile():
    a = len(files)
    f = files[int(a-1)]
    # print(f)
    loadCSV(f)


def initLoad():
    for f in files:
        if(f not in savedfile):
            loadCSV(f)
            print(f)
        else:
            print("already file")
            print(f)


initLoad()
