import psycopg2 as pg2
import os
import ftplib
from xml.dom import minidom

conn = pg2.connect(user="postgres", password="Pgis@rti2dss@2020",
                   host="119.59.125.134", database="envidb")
cur = conn.cursor()

# connect ftp
ftphost = '203.154.140.35'
ftp = ftplib.FTP(ftphost, 'Etc', 'Paris1234')
ftpdir = '/UM6972/XML/'
ftp.cwd(ftpdir)
files = ftp.nlst()

# check stock
cur.execute("SELECT filename FROM vibratexml")
rows = cur.fetchall()
savedfile = []
for r in rows:
    savedfile.append(r[0])


def insertDatabase(dat, fname):
    sql = f'''INSERT INTO vibratexml(
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
            {dat["tranppv"]},
            {dat["vertppv"]},
            {dat["longppv"]},
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


def extractNode(node):
    x = 0
    for n in node:
        x = n.getElementsByTagName("Value")[0].firstChild.data

    x = x.replace("&lt;", "")
    x = x.replace("&gt;", "")
    isNumber = any(map(str.isdigit, x))
    z = isNumber and x or 0
    # print(z)
    return z


def readfile(fpath, fname):
    xmldoc = minidom.parse(fpath)
    dat = {
        "eventtime": xmldoc.getElementsByTagName("EventTime")[0].firstChild.data,
        "eventdate": xmldoc.getElementsByTagName("EventDate")[0].firstChild.data,

        "tranppv": extractNode(xmldoc.getElementsByTagName("TranPPV")),
        "vertppv": extractNode(xmldoc.getElementsByTagName("VertPPV")),
        "longppv": extractNode(xmldoc.getElementsByTagName("LongPPV")),

        "tranppvponderated": extractNode(
            xmldoc.getElementsByTagName("TranPPVPonderated")),
        "vertppvponderated": extractNode(
            xmldoc.getElementsByTagName("VertPPVPonderated")),
        "longppvponderated": extractNode(
            xmldoc.getElementsByTagName("LongPPVPonderated")),

        "tranppvdb": extractNode(xmldoc.getElementsByTagName("TranPPVdB")),
        "vertppvdb": extractNode(xmldoc.getElementsByTagName("VertPPVdB")),
        "longppvdb": extractNode(xmldoc.getElementsByTagName("LongPPVdB"))
    }
    # print(dat)
    insertDatabase(dat, fname)


def loadfile(fname):
    fpath = r'/Users/sakdahomhuan/Dev/python_bropui_project/xml/'+fname
    ftp.retrbinary("RETR "+fname, open(fpath, 'wb').write)
    readfile(fpath, fname)
    os.remove(fpath)


def initLoadLastfile():
    a = len(files)
    f = files[int(a-1)]
    # print(f)
    if(f not in savedfile):
        loadfile(f)
        print(f)
    else:
        print("already file")
        print(f)


def initLoadAllFile():
    for f in files:
        if(f not in savedfile):
            loadfile(f)
            print(f)
        else:
            print("already file")
            print(f)


initLoadAllFile()
ftp.quit()
