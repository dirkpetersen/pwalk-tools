# Pwalk-hotspots

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
Query hotspots-dcw.sqlite for entire data consumption ....
Entire data consumption: 3153.541 TiB
Query hotspots-projects.sqlite for hotspots >= 10 GiB ....
Wrote hotspots-projects.csv with 43432 hotspots containing 2488.011 TiB total
  2488.011 TiB have not been accessed in 30 days (or 0.1 years)
  2226.474 TiB have not been accessed in 90 days (or 0.2 years)
  1438.751 TiB have not been accessed in 365 days (or 1.0 years)
  553.403 TiB have not been accessed in 1095 days (or 3.0 years)
  174.361 TiB have not been accessed in 1825 days (or 5.0 years)
  15.164 TiB have not been accessed in 3650 days (or 10.0 years)
  0.0 TiB have not been accessed in 5475 days (or 15.0 years)

```
