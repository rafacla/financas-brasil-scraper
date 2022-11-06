# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import zipfile

import pandas as pd
from scrapy.exceptions import DropItem
# useful for handling different item types with a single interface
from scrapy.pipelines.files import FilesPipeline


class FundosScraperPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        item['file_paths'] = file_paths
        # let's get the absolute path from the file
        absolute_path = os.path.join(self.store.basedir, file_paths[0])
        print(absolute_path)
        zf = zipfile.ZipFile(absolute_path)
        arquivo = zf.open(zf.filelist[0].filename)
        df = pd.read_csv(arquivo)
        return item
