from internetarchive import download
import pandas as pd
import py7zr
from os import listdir
from os.path import join as path_join
import os
import logging
import re
import xml.etree.ElementTree as ET


class BaseExtractor():
    """
    Base extractor class for ETL purposes. Contains methods per source and create a default staging, bronze filestructure to extract the files to.
    First extracts the data to the staging folder and then converts data to parquet when possible to store in bronze for consistency.   
    
    ...
    
    Attributes
    ----------
    path : str
        base path to data files
    staging_path : str
        path to staging files
    bronze_path : str
        path to bronze files
    
    Methods
    -------
    from_stackexchange(name):
        Download data from the stackexchange via archive.org
    """
    def __init__(self, path='./data'):
        """
        Parameters
        ----------
        path : str, optional
            base path to data files, by default './data'
        """
        self.path = path
        self.staging_path = path_join(path, 'staging')
        self.bronze_path = path_join(path, 'bronze')
        self.create_paths([self.staging_path, self.bronze_path])
        
    def create_paths(self, paths):
        """Create paths if they do not exist

        Parameters
        ----------
        paths : [str]
            list of paths to create
        """
        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)
        
    def from_stackexchange(self, name):
        """Download stackexchange archive

        Parameters
        ----------
        name : str
            name of stackexchange to download
        """
        # set paths.
        staging_path = path_join(self.staging_path, 'stackexchange')
        bronze_path = path_join(self.bronze_path, 'stackexchange', name)
        self.create_paths([staging_path, bronze_path])
        
        # set filename and download .7z from archive.
        filename = name + '.stackexchange.com.7z'
        download(identifier='stackexchange', files=filename, verbose=True, no_directory=True, destdir=staging_path)
        
        # unpack archive to staging folder.
        logging.info(f'Unpacking {filename}')
        with py7zr.SevenZipFile(path_join(staging_path, filename), 'r') as archive:
            archive.extractall(path=path_join(staging_path, name))
        
        # discover all XML files present in download and parse. Then save to bronze folder.
        xml_files = [file.split('.')[0] for file in listdir(path_join(staging_path, name))]
        for file in xml_files:
            df = pd.read_xml(path_join(staging_path, name, file + '.xml'), parser='lxml')
            # rename columns to convert from CamelCase to snake_case
            df = df.rename(columns=lambda col: re.sub(r'(?<!^)(?<![A-Z])(?=[A-Z])', '_', col).lower())
            df['archive_id'] = name
            df.to_parquet(path=path_join(bronze_path, file + '.parquet'))