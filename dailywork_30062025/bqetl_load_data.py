import os
from google.cloud import bigquery
import pandas as pd

df = pd.read_csv(r"C:\Users\Admin\Downloads\csv_files\movie_genre_classification_final.csv")

client = bigquery.Client()
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

# Director
df_director = df[['Director']].drop_duplicates().reset_index(drop=True)
df_director['director_key'] = df_director.index + 1
df_director = df_director[['director_key', 'Director']].rename(columns={'Director': 'director_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_director"
job = client.load_table_from_dataframe(df_director, table_id, job_config = job_config)
job.result()

# Actor
df_actor = df[['Lead_Actor']].drop_duplicates().reset_index(drop=True)
df_actor['actor_key'] = df_actor.index + 1
df_actor = df_actor[['actor_key', 'Lead_Actor']].rename(columns={'Lead_Actor': 'actor_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_actor"
job = client.load_table_from_dataframe(df_actor, table_id, job_config = job_config)
job.result()

# Production Company
df_company = df[['Production_Company']].drop_duplicates().reset_index(drop=True)
df_company['company_key'] = df_company.index + 1
df_company = df_company[['company_key', 'Production_Company']].rename(columns={'Production_Company': 'company_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_production_company"
job = client.load_table_from_dataframe(df_company, table_id, job_config = job_config)
job.result()

# Genre
df_genre = df[['Genre']].drop_duplicates().reset_index(drop=True)
df_genre['genre_key'] = df_genre.index + 1
df_genre = df_genre[['genre_key', 'Genre']].rename(columns={'Genre': 'genre_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_genre"
job = client.load_table_from_dataframe(df_genre, table_id, job_config = job_config)
job.result()

# Language
df_language = df[['Language']].drop_duplicates().reset_index(drop=True)
df_language['language_key'] = df_language.index + 1
df_language = df_language[['language_key', 'Language']].rename(columns={'Language': 'language_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_language"
job = client.load_table_from_dataframe(df_language, table_id, job_config = job_config)
job.result()

# Country
df_country = df[['Country']].drop_duplicates().reset_index(drop=True)
df_country['country_key'] = df_country.index + 1
df_country = df_country[['country_key', 'Country']].rename(columns={'Country': 'country_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_country"
job = client.load_table_from_dataframe(df_country, table_id, job_config = job_config)
job.result()

# Content Rating
df_rating = df[['Content_Rating']].drop_duplicates().reset_index(drop=True)
df_rating['content_rating_key'] = df_rating.index + 1
df_rating = df_rating[['content_rating_key', 'Content_Rating']].rename(columns={'Content_Rating': 'content_rating_name'})
table_id = "custom-sylph-458304-r4.peerisland.dim_content_rating"
job = client.load_table_from_dataframe(df_rating, table_id, job_config = job_config)
job.result()

df_fact = df.copy()
df_fact = df_fact.merge(df_director, left_on='Director', right_on='director_name', how='left')
df_fact = df_fact.merge(df_actor, left_on='Lead_Actor', right_on='actor_name', how='left')
df_fact = df_fact.merge(df_company, left_on='Production_Company', right_on='company_name', how='left')
df_fact = df_fact.merge(df_genre, left_on='Genre', right_on='genre_name', how='left')
df_fact = df_fact.merge(df_language, left_on='Language', right_on='language_name', how='left')
df_fact = df_fact.merge(df_country, left_on='Country', right_on='country_name', how='left')
df_fact = df_fact.merge(df_rating, left_on='Content_Rating', right_on='content_rating_name', how='left')
df_fact_final = df_fact[[
    'Title',
    'Year',
    'Description',
    'Duration',
    'Rating',
    'Votes',
    'Budget_USD',
    'BoxOffice_USD',
    'Num_Awards',
    'Critic_Reviews',
    'director_key',
    'actor_key',
    'company_key',
    'genre_key',
    'language_key',
    'country_key',
    'content_rating_key'
]].rename(columns={
    'Duration': 'duration_minutes',
    'Rating': 'rating',
    'Votes': 'votes',
    'Budget_USD': 'budget_usd',
    'BoxOffice_USD': 'box_office_usd',
    'Num_Awards': 'num_awards',
    'Critic_Reviews': 'critic_reviews'
})

table_id = "custom-sylph-458304-r4.peerisland.fact_movies"
job = client.load_table_from_dataframe(df_fact_final, table_id, job_config = job_config)
job.result()




