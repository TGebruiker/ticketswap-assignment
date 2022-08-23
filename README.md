# ticketswap-assignment

## 1. 
The system is located in the *stack_exchange_importer.py* file. For now you have to edit the parameters in the file itself but with a few easy steps you could implement an argument parser to be able to run the file from command line and and supply it with the necessary parameters. 

To use the system to extract data you need to follow the following steps:
- Supply the file with parameters, located at the bottom of the file
- Run the file

### Known limitations
Because I created the system to be as easy and fast as possible, there are some limitations:
- For now it is only possible to download stackexchange 7z files from archive.org. 
- The current tables used are: Badges, Comments, Posthistory, Postlinks, Posts, Tags, Users and Votes. To implement te rest you need to create the tables in the sql database.
- Column containing large text values are not inserted in the sql table because of issues with the insert statement getting too long. These columns are not interesting for the current data analysis so for now it is no problem.

