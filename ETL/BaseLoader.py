from os import listdir
from os.path import join as path_join
import pandas as pd
import sqlalchemy


class BaseLoader():
    """
    Base loader class for ETL purposes. Contains load methods.
    First loads the data from the silver folder and then loads the data into the desired destination.
    ...
    
    Attributes
    ----------
    path : str
        base path to data files
    silver_path : str
        path to silver files
    Methods
    -------
    transform(path, filename):
        Transform a folder or a single file with the added transformations.
    from_stackexchange(name):
        Download data from the stackexchange via archive.org.
    """
    def __init__(self, path='.\data'):
        """
        Parameters
        ----------
        path : str, optional
            base path to data files, by default './data'
        """
        self.path = path
        self.silver_path = path_join(path, 'silver')

    def load(self, path, filename):
        df = pd.read_parquet(path_join(path, filename))
        return df
        
    def to_sqlserver(self, path, username, password, server, port, database, chunksize=100):
        """Load data into SQL Server database.
        Maps the archive_id column from the name to an id, based on the archives table in SQL.

        Parameters
        ----------
        path : str
            path to files, relative to base path.
        username :  ste  
            SQL username
        password : str
            SQL password
        server : str
            SQL Server address
        port : str
            SQL Server port number
        database : str
            SQL Server database name
        chunksize : int, optional
            Chunksize for SQL inserts, by default 100
        """
        
        # Create SQL Alchemy engine and connect.
        silver_path = path_join(self.silver_path, path)
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver=SQL+Server+Native+Client+11.0')
        con = engine.connect()
        files = listdir(silver_path)
        for filename in files:
            df = self.load(silver_path, filename)
            # Select archives table and map values in df.
            archives_df = pd.read_sql(f'SELECT * FROM dbo.archives', con=con)
            archives_mapping = pd.Series(index=archives_df['archive_name'].values,data=archives_df['archive_id'].values).to_dict()
            df['archive_id'] = df['archive_id'].map(archives_mapping)
            tablename = filename.split('.')[0].lower()
            print(f'Exporting {path_join(silver_path, filename)} to sql database')
            # Insert table into SQL Server database
            df.to_sql(tablename, con=con, if_exists='append', index=False, chunksize=chunksize, method='multi')        
        con.close()