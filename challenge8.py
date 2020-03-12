import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config import db_password
import json
import re
import psycopg2
import time

#base path for files
file_dir ='C:/Users/RandallE/Class/Movies-ETL/'

#paths for each file
movies_json_path=f'{file_dir}/wikipedia.movies.json'
kaggel_csv_path=f'{file_dir}/movies_metadata.csv'
rating_csv_path = f'{file_dir}/ratings.csv'

# wikipedia movie data

wiki_movies_raw=" "

def wiki(movies_json):
    with open(movies_json, mode='r') as file:
        wiki_movies_raw = json.load(file)
        
    wiki_movies = [movie for movie in wiki_movies_raw
            if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]
    def clean_movie(movie):
            movie = dict(movie) #can use the same name due to scope
            alt_titles = {}
            # combine alternate titles
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune-Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles

            # merge names
            try: 
                def change_column_name(old_name, new_name):
                    if old_name in movie:
                        movie[new_name] = movie.pop(old_name)
                change_column_name('Adaptation by', 'Writer(s)')
                change_column_name('Country of origin', 'Country')
                change_column_name('Directed by', 'Director')
                change_column_name('Distributed by', 'Distributor')
                change_column_name('Edited by', 'Editor(s)')
                change_column_name('Length', 'Running time')
                change_column_name('Original release', 'Release date')
                change_column_name('Music by', 'Composer(s)')
                change_column_name('Produced by', 'Producer(s)')
                change_column_name('Producer', 'Producer(s)')
                change_column_name('Productioncompanies ', 'Production company(s)')
                change_column_name('Productioncompany ', 'Production company(s)')
                change_column_name('Released', 'Release Date')
                change_column_name('Release Date', 'Release date')
                change_column_name('Screen story by', 'Writer(s)')
                change_column_name('Screenplay by', 'Writer(s)')
                change_column_name('Story by', 'Writer(s)')
                change_column_name('Theme music composer', 'Composer(s)')
                change_column_name('Written by', 'Writer(s)')

                return movie    
            except Exception as e:
                print('check Rename Columns')
                print(e.message)
    
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    # remove duplicates
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')  
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except Exception as e:
        print('error duplicate rows line 67')
        print(e.message)
    
    # Remove columns if 90%    
    try:
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    except Exception as e:
        print('error Null Columns line 75')
        print(e.message)
    
    #Change data type and drop null rows
    try:
        box_office = wiki_movies_df['Box office'].dropna() 
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        #3: dollar signs
        try: 
            form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
            form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
            box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
            matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
            matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
        
        except Exception as e:
            print('error box office dollar line 91')
            print(e.message)
            
        def parse_dollars(s):
        #not a string then return NaN
            if type(s) != str:
                return np.nan
            # when form is $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                # remove dollar sign and million
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and make it millions
                value = float(s) * 10**6
                # return value
                return value
            #when form is $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float make it billions
                value = float(s) * 10**9
                # return value
                return value

            # when form is $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                # remove dollar sign, commas
                s = re.sub('\$|,','', s)
                # convert to float
                value = float(s)
                # return value
                return value
            # otherwise NaN
            else:
                return np.nan
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars) 
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    except Exception as e:
        print('error box office converstion line 130')
        print(e.message)    
              
    try:
        #4: Parsing Budget
        #1 Create a budget variable :
        budget = wiki_movies_df['Budget'].dropna()
        #2 Convert to String 
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
         #3 Remove dollar sign and -
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        #4 Match form one and two 
        matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    except Exception as e:
        print('error budget converstion line 148')
        print(e.message)
    
    try:
        #5: Parse Date
        #1  drop Nulls 
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        # 2 create variables
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
         #3 release date
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
    except Exception as e:
        print('error parse date line 164')   
        print(e.message)
    
    try:
        #6 runtimes
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        # dataframe is all string convert it to numeric values and Nan values fill to zero
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        # Convert the hour group to  minutes
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True) 
    except Exception as e:
        print('error runtime line 177')  
        print(e.message)
        
    return wiki_movies_df


def kaggel(kagg_csv):
    kaggle_metadata = pd.read_csv(kagg_csv,low_memory=False)
    
    try:
        #To remove bad data
        kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    except Exception as e:
        print('check error removing bad data conversion')
        print(e.message)  
        
        
    #Change Datatypes
    try: 
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except Exception as e:
        print('check error-for Run time conversion')  
        print(e.message)  
         
    return kaggle_metadata

def rate(rating_csv):
    ratings = pd.read_csv(rating_csv,low_memory=False)
    
    #Change datatypes
    try:
        pd.to_datetime(ratings['timestamp'], unit='s')
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    except Exception as e:
        print('check error-for datatype conversion')  
        print(e.message)  
        
    return ratings


def etl(movies_json,kagg_csv,rating_csv):
    
    movies_df = pd.merge(wiki(movies_json), kaggel(kagg_csv), on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # 1. drop  extra columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
            
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
   
    movies_df['video'].value_counts(dropna=False)
    
    # reorder columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    # Rename Columns to fit SQL Database
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    rating_counts = rate(rating_csv).groupby(['movieId','rating'], as_index=False).count() \
        .rename({'userId':'count'}, axis=1) \
        .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
     #use a left merge on movies_df:
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    movies_df.to_sql(name='movies', con=engine,if_exists='append')
    rating_counts.to_sql(name='ratings', con=engine, if_exists='append')
    
    return movies_df, rating_counts,movies_with_ratings_df


movie_data_df,rating_count_df,movie_w_rating_df=etl(movies_json_path,kaggel_csv_path,rating_csv_path)

#dont want it to display
#movie_w_rating_df

#dont want it to display
#rating_count_df

#dont want it to display
#movie_w_rating_df