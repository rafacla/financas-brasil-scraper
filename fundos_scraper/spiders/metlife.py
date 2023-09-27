import scrapy
from scrapy import FormRequest

import parameters
from database.database import Base, engine, get_db


class MetlifeSpider(scrapy.Spider):
    name = "metlife"
    start_urls = ['https://login.metlife.com.br/login/dynamic/Login.action']

    def __init__(self):
        # Create cursor, used to execute commands
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()

    def parse(self, response):
        yield FormRequest.from_response(response, formdata={'login': parameters.metlife_username,
                                                            'senha': parameters.metlife_password},
                                        callback=self.parse_after_login)

    def parse_after_login(self, response):
        yield response
