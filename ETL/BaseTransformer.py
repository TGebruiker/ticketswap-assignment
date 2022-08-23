import pandas as pd
from os.path import join as path_join
import os
from functools import partial, update_wrapper


class BaseTransformer():
    """
    Base transformer class for ETL purposes. Contains methods per transformation and create a default bronze, silver filestructure to move the files to.
    First loads the data from the bronze folder and then converts transforms data and stores it in the silver folder.   
    It is possible to transform a single file or transform a directory, this is decided when you call transform()
    ...
    
    Attributes
    ----------
    path : str
        base path to data files
    bronze_path : str
        path to bronze files
    silver_path : str
        path to silver files
    transformations : [partial methods]
        list of partial transformation methods, only needs the data argument to run
    Methods
    -------
    transform(path, filename):
        Transform a folder or a single file with the added transformations.
    from_stackexchange(name):
        Download data from the stackexchange via archive.org.
    """
    def __init__(self, path='./data'):
        """
        Parameters
        ----------
        path : str, optional
            base path to data files, by default './data'
        """
        self.path = path
        self.bronze_path = path_join(path,'bronze')
        self.silver_path = path_join(path,'silver')
        if not os.path.exists(self.silver_path):
            os.makedirs(self.silver_path)
        self.transformations = []
    
    def wrapped_partial(self, func, *args, **kwargs):
        """Updated partial method to be able to use the __name__ attribute

        Parameters
        ----------
        func : func
            function to create partial from

        Returns
        -------
        partial_func
            partial function with __name__ attribute added
        """
        partial_func = partial(func, *args, **kwargs)
        update_wrapper(partial_func, func)
        return partial_func

    def show_transformations(self):
        """Print the current transformations in the transformer class.
        """
        print([transformation.__name__ for transformation in self.transformations])
        
    def load(self, filepath):
        df = pd.read_parquet(path=filepath)
        return df
    
    def transform(self, path, filename=''):
        """Transform a single file or folder and move from bronze to silver folder.
        If you want to transform a single file, specify filename. 
        If you want to transform a folder, leave the filename out.

        Parameters
        ----------
        path : str
            path (relative to basepath) to files
        filename : str, optional
            filename (with extention), by default ''
        """
        bronze_path = path_join(self.bronze_path, path)
        silver_path = path_join(self.silver_path, path)
        if not os.path.exists(silver_path):
            os.makedirs(silver_path)
        if os.path.isdir(path_join(bronze_path, filename)):
            files = [f for f in os.listdir(bronze_path) if os.path.isfile(path_join(bronze_path, f))]
        else:
            files = [filename]
        for filename in files:
            df = self.load(path_join(bronze_path, filename))
            for transf in self.transformations:
                df = transf(df)
            df.to_parquet(path=path_join(silver_path, filename))
        
    def add(self, transformation, params):
        """Add a transformer to the class.

        Parameters
        ----------
        transformation : str
            the transformation you want to add
        params : any
            the parameters needed by the transformation
        """
        self.transformations.append(self.wrapped_partial(self.__getattribute__(transformation), params))
        
    def remove_columns(self, cols, df):
        """Remove columns transformation.
        When added to the transformer class, only supply with cols parameters, NOT the df parameter.    

        Parameters
        ----------
        cols : [str]
            list of columns to drop from DataFrame
        df : DataFrame
            the DataFrame to transform, do not fill in when using add(transformation, params)
            
        Returns
        -------
        DataFrame
            The transformed DataFrame
        """
        print(f'dropping columns: {cols}')
        df = df.drop(columns=cols)
        return df