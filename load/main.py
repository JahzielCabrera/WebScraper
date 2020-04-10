import argparse
import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd

from article import Article
from base import Base, Engine, Session

logger = logging.getLogger(__name__)

def main(filename):
    Base.metadata.create_all(Engine)
    session = Session()
    articles = pd.read_csv(filename)

    for index, row in articles.iterrows():
        logger.info('Loading article {} into DB'.format(row['uids']))
        article = Article(row['uids'], 
                          row['body'],
                          row['host'],
                          row['title'],
                          row['newspaper'],
                          row['n_tokens_body'],
                          row['n_tokens_title'],
                          row['url'])
        session.add(article)
    session.commit()
    session.close()
    
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)
    args = parser.parse_args()

    main(args.filename)
    