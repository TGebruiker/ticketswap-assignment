from internetarchive import download
import pandas as pd
import py7zr
from os import listdir
import logging
import sqlalchemy
import argparse
logging.getLogger().setLevel(logging.INFO)


class StackexchangeDownloader():
    """Simple class to download data from stackexchange archives located at archive.org, extract it and upload the data to a sql database.
    Uses index incrementation to preserve the relations in the sql database.
    
    ...
    
    Attributes
    ----------
    stackexchange_name : str
        Name of the stackexchange which is downloaded.
    engine : sqlalchemy engine
        Engine to connect to the sql database.
    downloaded_archive : dict(str, pd.DataFrame)
        Keys are table names and values are DataFrames containing the downloaded data.
    tables_with_ids : dict(str, str)
            Key is a table name and value is the name of the id column of that table in other tables.
    cols_to_drop : dict(str, [str])
        Key is a table name and value is a list of columns to drop before inserting in sql database.
    
    Methods
    -------
    download_from_stackexchange():
        Download the stackexchange from archive.org and put in the dowloaded_archive dict.
        
    export_to_sql(chunksize=100):
        Export the data to the sql database using pandas and sqlalchemy.

    """
    def __init__(self, stackexchange_name, user, password, server, port, database, tables_with_ids, cols_to_drop):
        """"
        Parameters
        ----------
        stackexchange_name : str
            Name of the stackexchange.
        user : str
            Username for sql database.
        password : str
            Password for sql database.
        server : str
            Sql server adress name.
        port : str
            Port number for sql database.
        database : str
            Sql database name.
        tables_with_ids : dict(str, str)
            Key is a table name and value is the name of the id column of that table in other tables.
        cols_to_drop : dict(str, [str])
            Key is a table name and value is a list of columns to drop before inserting in sql database.
        """
        self.stackexchange_name = stackexchange_name
        self.engine = self.create_engine(user, password, server, port, database)
        self.downloaded_archive = None
        self.tables_with_ids = tables_with_ids
        self.cols_to_drop = cols_to_drop
        
    def create_engine(self, user, password, server, port, database, dialect='mssql', driver = 'pyodbc', odbcdriver = 'SQL+Server+Native+Client+11.0'):
        """Creates the sqlalchemy engine."""
        engine = sqlalchemy.create_engine(f'{dialect}+{driver}://{user}:{password}@{server}:{port}/{database}?driver={odbcdriver}')
        return engine
        
    def download_from_stackexchange(self):
        """Download the stackexchange from archive.org and put in the dowloaded_archive dict."""
        # set filename and download .7z from archive.
        name = self.stackexchange_name
        filename = name + '.stackexchange.com.7z'
        download('stackexchange', files=filename, verbose=True)
        
        # unpack archive.
        logging.info(f'Unpacking {filename}')
        with py7zr.SevenZipFile('stackexchange/' + filename, 'r') as archive:
            archive.extractall(path=f"./stackexchange/{name}")
        
        # discover all XML files present in download and parse.
        tables = [name.split(sep='.')[0] for name in listdir(f'stackexchange/{name}')]
        archive = {table.lower():pd.read_xml(f'stackexchange/{name}/{table}.xml', parser='etree').rename(columns=lambda col: f'{table.lower()}_{col.lower()}') for table in tables}
        archive['posts']['posts_stackexchangename'] = name
        self.downloaded_archive = archive
        
    def reset_indexes(self):
        """Increment the table indexes (id column) to ensure the correctness of the data model when inserting into sql database."""
        archive = self.downloaded_archive
        con = self.engine.connect()
        columns_to_add = {} # dict containing the increment amount for every table.
        
        # check for every table what the maximum id value in the sql table is.
        for tablename, table in archive.items():
            id_name = self.tables_with_ids[tablename]
            max_index = pd.read_sql(f'SELECT MAX({tablename}_id) FROM dbo.{tablename}', con=con).iloc[0,0]
            if max_index is None:
                max_index = 0
            to_add = max_index + 1 - table[f'{tablename}_id'].min()
            logging.info(f'adding {to_add} to id column of {tablename}')
            table[f'{tablename}_id'] += to_add # add value to id column of tablename.
            columns_to_add[id_name] = to_add # save value to be added columns where id column of tablename has relationship.
            
        # add value to columns where id column of tablename has relationship.
        for colname, value in columns_to_add.items():
            for table in archive.keys():
                for col in archive[table].columns:
                    if col == f'{table}_{colname}':
                        logging.info(f'adding {value} to index of {table}.{col}')
                        archive[table][col] += value 
                        
        # add value to columns which do not conform to standard notation.                
        archive['posts']['posts_parentid'] += columns_to_add['postid'] 
        
        self.downloaded_archive = archive
        con.close()
    
    def drop_columns(self):
        for table in self.downloaded_archive.keys():
            cols_to_drop = self.cols_to_drop[table]
            logging.info(f'dropping {cols_to_drop} in {table}')
            self.downloaded_archive[table].drop(columns=self.cols_to_drop[table], inplace=True)
        
    def export_to_sql(self, chunksize = 100):
        """Export the data to the sql database using pandas and sqlalchemy."""
        if self.downloaded_archive == None:
            logging.info('No archive found, first download archive then export')
        else:
            self.reset_indexes()
            self.drop_columns()
            con = self.engine.connect()
            archive = self.downloaded_archive
            for tablename, table in archive.items():
                logging.info(f'Exporting {tablename} to sql database')
                table.to_sql(tablename, con=con, if_exists='append', index=False, chunksize=chunksize, method='multi')
            con.close()
        
def main():
    tables_with_ids = {
        'badges':'badgeid', 
        'comments':'commentid', 
        'posthistory':'posthistoryid', 
        'postlinks': 'postlinkid',
        'posts': 'postid',
        'tags': 'tagid',
        'users': 'userid',
        'votes': 'voteid'}
    cols_to_drop = {
        'badges' : [],
        'comments': ['comments_text','comments_userdisplayname'],
        'posthistory': ['posthistory_revisionguid', 'posthistory_text','posthistory_userdisplayname', 'posthistory_comment'],
        'postlinks': [],
        'posts': ['posts_body', 'posts_title', 'posts_tags'],
        'tags': [],
        'users': ['users_aboutme', 'users_location', 'users_profileimageurl', 'users_websiteurl'],
        'votes': []}
    
    user = 'admin'
    password = 'jf6qDce3CKCrDRrM'
    server = 'ticketswap-stackexchange-dwh.csboebiw4ypn.eu-west-3.rds.amazonaws.com'
    port = '1433'
    database = 'ticketswap-stackexchange-dwh'    
    archives_to_download = ['woodworking','ai']
    
    for archive in archives_to_download:
        logging.info(f'STARTING PROCESS FOR {archive.upper()}')
        downloader = StackexchangeDownloader(archive, user, password, server, port, database, tables_with_ids, cols_to_drop)
        downloader.download_from_stackexchange()
        downloader.export_to_sql()
        
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()    
    
    