import ftplib
import os
import pandas as pd


ftphost = '203.154.140.35'
ftp = ftplib.FTP(ftphost, 'Etc', 'Paris1234')
ftpdir = '/UM6972/CSV/'
ftp.cwd(ftpdir)

# ftp.retrlines()

files = ftp.nlst()

a = len(files)
f = files[int(a-1)]
fname = r'/Users/sakdahomhuan/Dev/python_bropui_project/csv/'+f
ftp.retrbinary("RETR "+f, open(fname, 'wb').write)
print(f)

df = pd.read_csv(
    r'/Users/sakdahomhuan/Dev/python_bropui_project/csv/UM6972_20210903090924.IDFW.csv', sep="\t", header=None)
