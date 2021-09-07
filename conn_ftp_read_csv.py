# import lib
import pandas as pd
import numpy as np
import ftplib
import csv
import psycopg2 as pg2

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


def insertData(fpath, fname):
    dat = {}
    with open(fpath) as fd:
        reader = csv.reader(fd)
        rows = [r for r in reader]

        for r in rows[:58]:
            x = r[1].split(" ")
            dat[r[0].lower()] = x[0]

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
            {dat["tranppv"].replace("<","")},
            {dat["vertppv"].replace("<","")},
            {dat["longppv"].replace("<","")},
            {dat["tranppvponderated"]},
            {dat["vertppvponderated"]},
            {dat["longppvponderated"]},
            {dat["tranppvdb"]},
            {dat["vertppvdb"]},
            {dat["longppvdb"]}
        )'''
    # print(sql)

    cur.execute(sql)
    conn.commit()


def loadCSV(fname):
    fpath = r'/Users/sakdahomhuan/Dev/python_bropui_project/csv/'+fname
    ftp.retrbinary("RETR "+fname, open(fpath, 'wb').write)
    print(fname)
    insertData(fpath, fname)


# retrieve file
def initLoop():
    a = len(files)
    f = files[int(a-1)]
    loadCSV(f)


def initLoop2():
    for f in files:
        if(f not in savedfile):
            loadCSV(f)
            print(f)
        else:
            print("already file")
            print(f)


initLoop2()
