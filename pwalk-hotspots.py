#! /usr/bin/env python3

import sys, os, pwd, grp, sqlite3, csv, datetime

db='hotspots.sqlite' # default database name if not on command line
thresholdGB=10    # >= folder size in GiB that is considered a hotspot
daysaged=[30,90,365,1095,1825,3650,5475]  # space historam by age
TiB=1099511627776 # bytes in a Tebibyte

def getusr(uid):
    try:
        return pwd.getpwuid(uid)[0]
    except:
        return uid

def getgrp(gid):
    try:
        return grp.getgrgid(gid)[0]
    except:
        return gid

def days(unixtime):
    diff=datetime.datetime.now()-datetime.datetime.fromtimestamp(unixtime)
    return diff.days
 
if len(sys.argv) > 1:
    db=sys.argv[1]
if not os.path.exists(db):
    print(f"Path {db} does not exist")
    sys.exit(1)
conn = sqlite3.connect(db) 
c = conn.cursor()
#c.execute("attach 'hotspots.sqlite' as hs")
c.execute('pragma journal_mode = MEMORY;')
c.execute('pragma synchronous = NORMAL;')
c.execute('pragma temp_store = MEMORY;')
c.execute('pragma mmap_size = 30000000000;')

print ('Connected to sqlite3 version %s' % sqlite3.version)
print(f"Query {db} for entire data consumption ....")
c.execute('select sum(pw_dirsum) from hotspots')
entirebytes=c.fetchall()[0][0]
print(f" Entire data consumption: {round(entirebytes/TiB,3)} TiB")

print(f"Query {db} for hotspots >= {thresholdGB} GiB ....")
rows = c.execute(f"select * from hotspots where GiB >= {thresholdGB}")
# filename,UID,GID,st_atime,st_mtime,pw_fcount,pw_dirsum,TiB,GiB,MiBAvg
header=[d[0] for d in c.description]
header[0]="folder"
header[1]="user"
header[2]="group"
header[3]="days_acc"
header[4]="days_mod"

totalbytes=0
agedbytes=[]
for i in daysaged:
    agedbytes.append(0)
numhotspots=0

mycsv=db.replace('.sqlite','.csv')
if mycsv == db:
    mycsv = 'hotspots.csv'

with open(mycsv, 'w') as f:
    writer = csv.writer(f, dialect='excel')
    writer.writerow(header)
    for r in rows:
        row = list(r)       
        row[1]=getusr(row[1])
        row[2]=getgrp(row[2])
        row[3]=days(row[3]) 
        row[4]=days(row[4])
        writer.writerow(row)
        numhotspots+=1
        totalbytes+=row[6]
        for i in range(0,len(daysaged)): 
            if row[3] > daysaged[i]:
                agedbytes[i]+=row[6]
 
    #writer.writerows(rows)

conn.commit()
conn.close()

print(f" Wrote {mycsv} with {numhotspots} hotspots containing {round(totalbytes/TiB,3)} TiB total")
for i in range(0,len(daysaged)):
    print(f"  {round(agedbytes[i]/TiB,3)} TiB have not been accessed for {daysaged[i]} days (or {round(daysaged[i]/365,1)} years)") 
