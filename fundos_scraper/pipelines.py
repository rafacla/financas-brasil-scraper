# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
import zipfile


class FundosScraperPipeline(FilesPipeline):
 def file_path(self, request, response=None, info=None):
  file_name: str = request.url.split("/")[-1]
  return file_name

 def item_completed(self, results, item, info):
  file_paths = [x['path'] for ok, x in results if ok]
  if not file_paths:
   raise DropItem("Item contains no files")
  item['file_paths'] = file_paths
  return item
