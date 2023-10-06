#! /usr/bin/env python3

"""
pwalk-info aggregates one or multiple pwalk output files and provides
summary information about the files in the file system or generates
a list of duplicates

- the 'total' subcommand prints the total number of bytes and GiB in the csv file
- the 'filetypes' subcommand groups by file extension and space usage
- the 'duplicates' subcommand finds duplicate files in different paths
  that have the same filename, modification time and size

To add more commands add a "subparsers.add_parser" section in the 
parse_arguments() function and add the corresponding 
"if args.subcmd in ['command', 'cmd']:" section in the main() function

"""

# internal modules
import sys, os, argparse, csv, platform, textwrap, inspect
if sys.platform.startswith('linux'):
    import getpass, pwd, grp
# stuff from pypi
try:
    import duckdb
except:
    print('Could not import module "duckdb". Please run "python3 -m pip install --upgrade --user duckdb"')
    sys.exit(1)

__app__ = 'pwalk info'
__version__ = '0.0.1'

def main():

    #remove trailing slash
    if args.csvpath.endswith('/'):
        args.csvpath = args.csvpath[:-1]

    # Initialize DuckDB connection
    cores = int(os.getenv('SLURM_CPUS_ON_NODE', args.cores))
    threads = cores*2
    conn = duckdb.connect(':memory:')
    conn.execute(f'PRAGMA threads={threads};')

    if os.path.isdir(args.csvpath):
        #print('Using:', args.csvpath)
        # if dir, create a union view over all the CSV files in the directory
        print('find csv files ....', flush=True)
        csv_files = conn.execute(f"SELECT * FROM glob('{args.csvpath}/*.csv')").fetchall()
        #print('csv_files', csv_files)
        query_parts = [f"SELECT * FROM read_csv_auto('{csv_file[0]}')" for csv_file in csv_files]
        union_query = " UNION ALL ".join(query_parts)
    elif os.path.isfile(args.csvpath):
        union_query = f"SELECT * FROM read_csv_auto('{args.csvpath}')"
    else:
        print(f"Path {args.csvpath} is not a file or directory")
        return False
    print("Execute query:", union_query)
    conn.execute(f"CREATE VIEW combined_csvs AS {union_query}")
    # Now you can query the combined data from all CSV files directly
    print('Fetch result ....', flush=True)


    # ***************************************************************
    if args.subcmd in ['total', 'tot']:
    
        rows = conn.execute(f"""
            SELECT SUM(st_size) from combined_csvs where pw_dirsum=0
            """).fetchall()

        total = rows[0][0]
        
        print("Total Bytes:", total)
        print("Total   GiB:", round(total/1024/1024/1024,3))


    # ***************************************************************
    if args.subcmd in ['filetypes', 'typ']:
    
        rows = conn.execute(f"""
            SELECT fileExtension,
                SUM(st_size) as Bytes
            FROM combined_csvs
            where pw_dirsum=0 AND st_size>0
            group by fileExtension
            order by Bytes desc
            """).fetchall()

        total = 0
        for row in rows:
            total+=row[1]        
        
        print('\nExtension, %, Bytes')
        cnt = 0
        try: 
            for row in rows:
                print(f'{row[0]}, {row[1]/total:.2f}, {row[1]}')
                cnt+=1
        except BrokenPipeError:
            # Pipe is broken (e.g., when output is piped to 'head'). Exit silently.
            sys.stdout.close()
            os._exit(0)
        except Exception as e:
            print(f'Error: {e}')
            sys.exit(1)
        
        print('\n----------------------------')
        print("Total File types:", cnt)
        print("Total Bytes:", total)
        print("Total GiB:", round(total/1024/1024/1024,3))

    # ***************************************************************
    if args.subcmd in ['duplicates', 'dup']:
    
        dedupquery=f"""
                SELECT
                    -- Extract the filename without path 
                    SUBSTRING(
                        filename FROM LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2
                        FOR 
                        LENGTH(filename) - POSITION('/' IN REVERSE(filename)) - POSITION('.' IN REVERSE(filename)) + 1
                    ) AS plain_file_name,         
                    st_mtime,
                    st_size,
                    COUNT(*) as duplicates_count,
                    ARRAY_AGG(filename) as duplicate_files  -- Collect the full paths of all duplicates
                FROM
                    combined_csvs
                WHERE 
                    filename NOT LIKE '%/miniconda3/%' AND
                    filename NOT LIKE '%/miniconda2/%' AND    -- let's ignore all that miniconda stuff
                    st_size > 1024*1024                       -- we only look at files > 1 MB
                GROUP BY
                    plain_file_name, 
                    st_mtime,
                    st_size
                HAVING
                    COUNT(*) > 1  -- Only groups with more than one file are duplicates
                ORDER BY
                    duplicates_count DESC;          
            """
        
        print(f'{dedupquery}\n\nWrite query result to {args.outfile} ...', flush=True)
        column_names = ['filename', 'modified', 'bytesize', 'no', 'duplicates'] #[desc.name for desc in conn.description()]
        print(f'Column names: {column_names}')
        rows = conn.execute(dedupquery).fetchall()
        
        extrabytes = 0
        # Write the results to a CSV file using the csv module
        with open(args.outfile, 'w', newline='') as file:
            writer = csv.writer(file, dialect='excel')            
            writer.writerow(column_names)            
            for row in rows:
                writer.writerow(row)
                extrabytes+=row[2]*(row[3]-1) 

        print(f'Extra/duplicate data: {extrabytes} Bytes or {extrabytes/1024/1024/1024:.3f} GiB')
        
def parse_arguments():
    """
    Gather command-line arguments.
    """       
    parser = argparse.ArgumentParser(prog='pwalk-info ',
        description='Provide some details on one or multiple pwalk output files')
    parser.add_argument( '--debug', '-d', dest='debug', action='store_true', default=False,
        help="verbose output for all commands")
    parser.add_argument('--cores', '-c', dest='cores', action='store', default='16', 
        help='Number of cores to be allocated for duckdb (default=16)')
    parser.add_argument('--version', '-v', dest='version', action='store_true', default=False, 
        help='print version info')
    
    subparsers = parser.add_subparsers(dest="subcmd", help='sub-command help')

    # ***
    # parser_config = subparsers.add_parser('config', aliases=['cfg'], 
    #     help=textwrap.dedent(f'''
    #         Print a pwalk report             
    #     '''), formatter_class=argparse.RawTextHelpFormatter)
    # parser_config.add_argument( '--monitor', '-m', dest='monitor', action='store', default='',
    #     metavar='<email@address.org>', help='setup as a monitoring cronjob ' +
    #     'on a machine and notify an email address')

     # ***************************************************************    
    parser_total = subparsers.add_parser('total', aliases=['tot'], 
        help=textwrap.dedent(f'''
            print the total number of bytes and GiB in the csv file             
        '''), formatter_class=argparse.RawTextHelpFormatter)
    parser_total.add_argument('csvpath', action='store',
        help='csv path can be a file or a folder')

     # ***************************************************************    
    parser_filetypes = subparsers.add_parser('filetypes', aliases=['typ'], 
        help=textwrap.dedent(f'''
            Print a pwalk report by file extension             
        '''), formatter_class=argparse.RawTextHelpFormatter)
    parser_filetypes.add_argument('csvpath', action='store',
        help='csv path can be a file or a folder')
    
     # ***************************************************************
    parser_duplicates = subparsers.add_parser('duplicates', aliases=['dup'], 
        help=textwrap.dedent(f'''
            Export a new csv file with information about duplicate files             
        '''), formatter_class=argparse.RawTextHelpFormatter)
    parser_duplicates.add_argument( '--outfile', '-o', dest='outfile', action='store', default='duplicates.csv',
        metavar='<./duplicates.csv>', help='report file that contains all the dups')
    parser_duplicates.add_argument('csvpath', action='store',
        help='csv path can be a file or a folder')
    
    

    # ***************
    args = parser.parse_args()
    if len(sys.argv) == 1 or not args.subcmd or not 'csvpath' in args:
        parser.print_help(sys.stdout)  
        sys.exit(1)

    return args

if __name__ == "__main__":
    try:
        args = parse_arguments() 
        if main():
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print('\nExit !')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)                