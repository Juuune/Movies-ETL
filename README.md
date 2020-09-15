# Movies ETL Analysis

## Overview

Amazing Prime is a virtual online video service company and this client want to know which movie is popurlar and well performed in the states. For this, extracted movies data from wiki-pedia and kaggle into proper dataform, transformed data with eliminating duplication & na, extracting info with regular expression, and merge into single dataframe. Lastly data was loaded into SQL database. 

Now the challenge is to create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables.

## Challenge Objectives
The goal of this challenge are to :
- Create an automated ETL pipeline.
- Extract data from multiple sources.
- Clean and transform the data automatically using Pandas and regular expressions.
- Load new data into PostgreSQL.

## Resolution 

### Assumption 1: 3 imported data has same format .csv
- Try to convert .csv file into pandas dataframe using pd.read_csv 
- except 'FileNotFoundError' which means that file is not in .csv format 
- for the 'FileNotFoundError', load raw data using 'with open' and 'json.load()'
```   
 try:
     kaggle_metadata = pd.read_csv(f'{file_dir}/{kaggle}.csv', low_memory = False) 
     print('kaggle data converted')
     ratings= pd.read_csv(f'{file_dir}/{rating}.csv', low_memory = False)       
     print('rating data converted')
     wiki_df = pd.read_csv(f'{file_dir}/{wiki}.csv', low_memory = False)   
     print('wiki data converted')
 except FileNotFoundError:    
        with open(f'{file_dir}/{wiki}.json', mode='r') as f:
             raw = json.load(f)
        print('wiki data converted')
        pass
```

### Assumption 2: wiki data clean function will work without error 
- Add all the transforming work into clean_movie function and and apply function to clean up with 'try' 
- except NameError, that previously assigned column name is incorrect or new columns has been added to wiki data 
```
 try:
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
 except NameError:
        pass
```
### Assumption 3: At least one data of release_date_wiki is not match with release_date_kaggle  
- Based on previous analysis this clean process has added, since df.drop only drop existing value if there's no mismatch data the process can be ignored
```
 try:
     movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & \
     (movies_df['release_date_kaggle'] < '1965-01-01')].index)
 except KeyError:
        pass 
```
### Assumption 4: Load movies_df data into SQL database for the first time without errors
- Load ddataframe to sql except ValueError that the table has already made, append rows to existing table 
- execute TRUNCATE (table); before loading for preventing errors
```
 try: 
     movies_df.to_sql(name='movies', con=engine)
     print('Upload movies_df to sql')
 except ValueError: 
        movies_df.to_sql(name='movies', con=engine, if_exists='append')
        print('Upload movies_df to sql')
 except:   
       print('failed to upload') 
       pass   
```
### Assumption 5: Load movies_with_rating data into SQL database for the first time without errors
- Load ddataframe to sql except ValueError that the trial has failed, pass and continue with next chunk
```
  try: 
      data.to_sql(name='ratings', con=engine, if_exists='append')
      rows_imported += len(data)
      print(f'Done.{time.time() - start_time} total seconds elapsed')
  except ValueError:
         print('failed to upload') 
         pass 
```
