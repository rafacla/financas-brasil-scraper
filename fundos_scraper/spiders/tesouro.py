from datetime import datetime

from database.database import engine, Base, get_db
import scrapy

import parameters
from fundos_scraper.items import TesouroDiretoItem


class MesesSpider(scrapy.Spider):
    name = "tesouro"
    start_urls = ['https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3']

    def __init__(self):
        # Create cursor, used to execute commands
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()

    def parse(self, response):
        # Recupera links disponíveis na pagina da CVM
        links = response.css('a::attr(href)').getall()
        for i, link in enumerate(links):
            if link[-3:] == 'csv':
                file_url = response.urljoin(link)
                item = TesouroDiretoItem()
                item['pipeline'] = 'tesouro'
                item['file_urls'] = [file_url]
                yield item
