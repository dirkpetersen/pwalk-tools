# Pwalk Tools

Pwalk tools offer post-processing and analysis for [John's pwalk](https://github.com/fizwit/filesystem-reporting-tools). If you do not already have a pwalk csv file you can analyse, the best way to generate a clean pwalk csv files is using Froster (https://github.com/dirkpewersen/froster). Run these commands to install Froster and prepare it for indexing. 
Note: If you decide to use Pwalk directly and not via Froster you might have to clean up some incompatible characters. Use `iconv -f ISO-8859-1 -t UTF-8 ./old-file.csv > ./new-file.csv` to prepare for the use with DuckDB

```
curl https://raw.githubusercontent.com/dirkpetersen/froster/main/install.sh | bash
froster config --index
```

Now let's scan a folder /shared/my_department and generate a csv file my_department.csv in the home directory 

```
froster index --pwalk-copy ~/my_department.csv /shared/my_department
```

## pwalk-info

`pwalk-info.py` can analyse pwalk output csv files with Posix file system metadata. You can either pass an individual pwalk.csv file as an argument or an entire folder with multiple pwalk csv files. (this will only work if all csv files in a folder are pwalk csv files). `pwalk-info.py` has serveral sub commands.

First, download pwalk-info.py, make the script executable and install the duckdb dependency

```
wget https://raw.githubusercontent.com/dirkpetersen/pwalk-tools/main/pwalk-info.py
chmod +x pwalk-info.py
python3 -m pip install --upgrade --user duckdb
```


To find out information about duplicate files run this command 

```
./pwalk-info.py dup --outfile duplicates-test.csv ./pwalk-test.csv
./pwalk-info.py dup --outfile duplicates-dep.csv ~/my_department.csv
./pwalk-info.py dup --outfile duplicates-folder.csv ~/my_folder
```

A duplicate occurs if 2 or more files stored in different directories have the same file name, modification time stamp and file size, see this example

```
cat duplicates-test.csv
filename,modified,bytesize,no,duplicates
UNICORN 7.0.0.953.zip,1689184019,1322724610,2,"['/home/groups/Vollum/Labs/ReichowLab/UNICORN 7.0.0.953.zip', '/home/groups/Vollum/Labs/ReichowLab/TEMP/UNICORN 7.0.0.953.zip']"
```

to simply see file system space consumption run this:

```
./pwalk-info.py tot ./pwalk-test.csv
```

and to see what share of file types you have in your pwalk csv run this

```
$ ./pwalk-info.py typ ./pwalk-test.csv | head

Execute query: SELECT * FROM read_csv_auto('./pwalk-test.csv')
Fetch result ....

Extension, %, Bytes
zip, 0.95, 2645449220
pdf, 0.05, 145320001
, 0.00, 34856
xlsx, 0.00, 23880
lic, 0.00, 1728
ACC, 0.00, 202
```


also check the help page: 

```
./pwalk-info.py --help
usage: pwalk-info  [-h] [--debug] [--cores CORES] [--version] {total,tot,filetypes,typ,duplicates,dup} ...

Provide some details on one or multiple pwalk output files

positional arguments:
  {total,tot,filetypes,typ,duplicates,dup}
                        sub-command help
    total (tot)         print the total number of bytes and GiB in the csv file
    filetypes (typ)     Print a pwalk report by file extension
    duplicates (dup)    Export a new csv file with information about duplicate files

optional arguments:
  -h, --help            show this help message and exit
  --debug, -d           verbose output for all commands
  --cores CORES, -c CORES
                        Number of cores to be allocated for duckdb (default=16)
  --version, -v         print version info
```

## pwalk-hotspots

**find folders in your Posix file system that use a lot of space** 

We use csv files created by [John's pwalk](https://github.com/fizwit/filesystem-reporting-tools) and extract the folder records that contain file counts and space consumption into a sqlite database. After that we extract a csv file with all folders > 10 GiB from the sqlite database. 

You can load the resulting csv file in Excel. It is sorted by size and shows the largest folders first, along with number of files, average file size and days that the data has not been accessed: 

<img width="506" alt="image" src="https://user-images.githubusercontent.com/1427719/202926384-371f35ac-3a90-4d2b-a38a-cf023a9ddd7f.png">


## overview

- Run [John's pwalk](https://github.com/fizwit/filesystem-reporting-tools) on each top level folder in your file system and store the resulting csv files in a single folder. You can run this as root on your HPC cluster 
- run pwalk-import.py and pass the folder name that contains all the csv files from pwalk as argument 
- pwalk-import.py created a database file hotspots.sqlite in the current folder 
- rename hotspots.sqlite to something useful, e.g. hotspots-projects.sqlite
- run pwalk-hotspots.py and pass the database file as argument, e.g. hotspots-projects.sqlite

## prepare data

```
[dp@cpt1 pwalk-hotspots]$ ./pwalk-import.py /projects/pwalk/csv

Connected to sqlite3 version 2.6.0
0.006 GiB total: Reading /projects/pwalk/csv/orMetagenome.csv ...
  ... inserting df in DB
  ... cleaning up ...
1.585 GiB total: Reading /projects/pwalk/csv/rna_seq_gen.csv ...
  ... inserting df in DB
  ... cleaning up ...
1.849 GiB total: Reading /projects/pwalk/csv/miller_lab.csv ...  
```

## extract data

```
[dp@cpt1 pwalk-hotspots]$ ./pwalk-hotspots.py hotspots-projects.sqlite

Connected to sqlite3 version 2.6.0
Query hotspots-dcw.sqlite for entire data consumption ....
Entire data consumption: 3153.541 TiB
Query hotspots-projects.sqlite for hotspots >= 10 GiB ....
Wrote hotspots-projects.csv with 43432 hotspots containing 2488.011 TiB total
  2347.124 TiB have not been accessed for 30 days (or 0.1 years)
  2226.474 TiB have not been accessed for 90 days (or 0.2 years)
  1438.751 TiB have not been accessed for 365 days (or 1.0 years)
  553.403 TiB have not been accessed for 1095 days (or 3.0 years)
  174.361 TiB have not been accessed for 1825 days (or 5.0 years)
  15.164 TiB have not been accessed for 3650 days (or 10.0 years)
  0.0 TiB have not been accessed for 5475 days (or 15.0 years)
```

