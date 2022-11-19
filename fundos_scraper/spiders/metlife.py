import mysql.connector
import scrapy
from scrapy import Request, FormRequest

import parameters


class MetlifeSpider(scrapy.Spider):
    name = "metlife"
    start_urls = ['https://login.metlife.com.br/login/dynamic/Login.action']

    def __init__(self):
        self.conn = mysql.connector.connect(
            host=parameters.host,
            user=parameters.user,
            password=parameters.password,
            database=parameters.database
        )
        # Create cursor, used to execute commands
        self.cur = self.conn.cursor()

    def parse(self, response):
        yield FormRequest.from_response(response, formdata={'login': parameters.metlife_username,
                                                            'senha': parameters.metlife_password},
                                        callback=self.parse_after_login)

    def parse_after_login(self, response):
        yield response
