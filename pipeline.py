#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.INFO)
import subprocess

logger = logging.getLogger(__name__)
news_sites = ['eluniversal', 'diariodexalapa']

def main():
    _extract()
    _transform()
    _load()

def _extract():
    logger.info('Starting extract process')
    for news_site in news_sites:
        subprocess.run(['python3.7', 'main.py', news_site], cwd='./extract')
        subprocess.run(['find', '.', '-name', '{}*'.format(news_site),
                        '-exec', 'mv', '{}', '../transform/{}_.csv'.format(news_site),
                        ';'], cwd='./extract')

def _transform():
    logger.info('Starting transform process')
    for news_site in news_sites:
        dirty_data_filename = '{}_.csv'.format(news_site)
        clean_data_filename = 'clean_{}'.format(dirty_data_filename)
        subprocess.run(['python3.7', 'main.py', dirty_data_filename], cwd='./transform')
        subprocess.run(['rm', dirty_data_filename], cwd='./transform')
        subprocess.run(['mv', clean_data_filename, '../load/{}.csv'.format(news_site)], cwd='./transform')

def _load():
    logger.info('Satarting load process')
    for news_site in news_sites:
        clean_data_filename = '{}.csv'.format(news_site)
        subprocess.run(['python3.7', 'main.py', clean_data_filename], cwd='./load')
        subprocess.run(['rm', clean_data_filename], cwd='./load')

if __name__=='__main__':
    main()