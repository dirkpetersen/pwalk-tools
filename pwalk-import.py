#! /usr/bin/env python3

import sys, os, sqlite3, glob
from pkg_resources import parse_version

mode='pandas'
pd_min_version='1.3'

if mode == 'pandas':
    try:
        import pandas
    except: 
        print ("No pandas installed, please run: ")
        print (" python3 -m pip install --user --upgrade pandas")
        sys.exit(1)
    if parse_version(pandas.__version__) <  parse_version(pd_min_version):
        print ("Your version of pandas is too old, please update: ")
        print (" python3 -m pip install --user --upgrade pandas")
        sys.exit(1)

elif mode == 'dask':
    try:
        import dask.dataframe
    except:
        print ("No Dask installed, please run: ")
        print (" python3 -m pip install --user --upgrade dask")
        sys.exit(1)

if len(sys.argv) == 1:
    print('Run %s <csv-folder>' % sys.argv[0])
    sys.exit()    
csvfld = sys.argv[1]
if not os.path.exists(csvfld):
    print('Folder %s does not exist' % csvfld)
    sys.exit()
conn = sqlite3.connect(':memory:') 
conn2 = sqlite3.connect('hotspots.sqlite') 
print ('Connected to sqlite3 version %s' % sqlite3.sqlite_version) 
conn2.close()

c = conn.cursor()

c.execute("attach 'hotspots.sqlite' as hs")
c.execute('pragma journal_mode = MEMORY;')
c.execute('pragma synchronous = NORMAL;')
c.execute('pragma temp_store = MEMORY;')
c.execute('pragma mmap_size = 30000000000;')

c.execute('''
    create table if not exists pwalks (
        inode integer primary key,
        p_inode integer,
        d_depth integer,
        filename text,
        fileExtension text,
        UID integer,
        GID integer,
        st_size integer,
        st_dev integer,
        st_blocks integer,
        st_nlink integer,
        st_mode integer,
        st_atime integer,
        st_mtime integer,
        st_ctime integer,
        pw_fcount integer,
        pw_dirsum integer)
''') 
conn.commit()

totsize=0
if mode == 'pandas':
    if os.path.isfile(csvfld):
        files = []
        files.append(csvfld)
    else:
        files = glob.glob("%s/*.csv" % csvfld)
    for filen in files:
        totsize=totsize+os.path.getsize(filen)
        print('%s GiB total: Reading %s ...' % (round(totsize/1073741824,3), filen), flush=True)
        df = pandas.read_csv(filen, low_memory=False, encoding_errors='ignore') 
                       #encoding='ISO-8859-1' # encoding_errors='ignore'
        print ('  ... inserting df in DB ', flush=True)
        df.to_sql('pwalks', conn, if_exists='append',
                 index=False) # , dtype={'inode': 'INTEGER PRIMARY KEY'}
        print ('  ... cleaning up ...', flush=True)
        c.execute('delete from pwalks where pwalks.pw_fcount <= 0')
elif mode == 'dask':
    print('Reading csv from %s ...' % csvfld, flush=True)
    df = dask.dataframe.read_csv("%s/*.csv" % csvfld, blocksize=64e6) #encoding_errors='ignore', low_memory=False)
    df.compute()
    #print(df.head(), flush=True)
    # Not implemented 
else:
    print('Please set mode to "pandas" or "dask"')

print('Creating hotspots table...')
c.execute('''
create table hs.hotspots AS select
    pwalks.filename,
    pwalks.UID,
    pwalks.GID,
    pwalks.st_atime,
    pwalks.st_mtime,
    pwalks.pw_fcount,
    pwalks.pw_dirsum,
    pwalks.pw_dirsum/1099511627776 as TiB,
    pwalks.pw_dirsum/1073741824 as GiB,
    pwalks.pw_dirsum/1048576/pwalks.pw_fcount as MiBAvg    
From
    pwalks
Where 
    pwalks.pw_fcount > 0    
Order By
    pwalks.pw_dirsum Desc
''')

print('Done!')

conn.commit()
conn.close()
