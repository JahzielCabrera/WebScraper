import argparse
import hashlib
import logging
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse

import pandas as pd
import nltk
from nltk.corpus import stopwords

logger = logging.getLogger(__name__)

def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper = _extract_newspaper(filename)
    df = _add_newspaper_column(df, newspaper)
    df = _extract_newspaper_host(df, newspaper)
    df = _fill_missing_titles(df)
    df = _add_uids(df)
    df = _stripped_title_body(df, 'title')
    df = _stripped_title_body(df, 'body')
    df = _tokenize_title_body(df)
    df = _remove_duplicates_entries(df, 'title')
    df = _drop_rows_with_missing_values(df)
    _save_file(df, filename)

    return df

def _drop_rows_with_missing_values(df):
    logger.info('Dropping rows with missing values')
    return df.dropna()

def _save_file(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Saving data at location {}'.format(clean_filename))
    df.to_csv(clean_filename)

def _remove_duplicates_entries(df, column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)

    return df

def _tokenize_column(df, column_name): 
    stop_words = set(stopwords.words('spanish'))
    tokens_column = (df
                .dropna()
                .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
                .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                .apply(lambda tokens: list(map(lambda letter: letter.lower(), tokens)))
                .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                .apply(lambda valid_word_list: len(valid_word_list)) 
             )

    return tokens_column

def _tokenize_title_body(df):
    df['n_tokens_title'] = _tokenize_column(df, 'title')
    df['n_tokens_body'] = _tokenize_column(df, 'body')
    return df

def _stripped_title_body(df, column_name):
    logger.info('Stripping {}'.format(column_name))

    stripped_title_body = (df
                     .apply(lambda row: row[column_name], axis=1)
                     .apply(lambda body: list(body))
                     .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ''), letters)))
                     .apply(lambda letters: ''.join(letters))
                     )
    df[column_name] = stripped_title_body
    return df

def _add_uids(df):
    logger.info('Generating Id for each row')

    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uids'] = uids
    df.set_index('uids', inplace=True)
    return df

def _fill_missing_titles(df):
    logger.info('Filling missing titles')

    missing_titles_mask = df['title'].isna()
    missing_titles = (df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.split('-'))
                        .applymap(lambda title_word_list: ' '.join(title_word_list)))

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df

def _extract_newspaper_host(df, newspaper):
    logger.info('Extracting newspaper host from url')

    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _add_newspaper_column(df, newspaper):
    logger.info('Filling the newspaper column with {}'.format(newspaper))
    df['newspaper'] = newspaper
    return df

def _extract_newspaper(filename):
    logger.info('Extracting newspaper')
    newspaper = filename.split('_')[0]

    logger.info('Newspaper is detected: {}'.format(newspaper))
    return newspaper

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))
    return pd.read_csv(filename)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help = 'The path to the dirty data',
                        type = str)
    args = parser.parse_args()
    df = main(args.filename)
    print(df)