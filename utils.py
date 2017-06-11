import pymongo
from scrapy.dupefilters import RFPDupeFilter
from scrapy.utils.job import job_dir
import logging

from .common.utils import find_class

logger = logging.getLogger(__name__)

"""
SETTINGS

# 这里路径需要填补完全，否则会出现 item 和 ItemClass 类型不匹配的情况
from {project}.items import {ItemClass}

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'scrapy'

ITEM_PIPELINES = {
  'scrapy_dupefilter_util.DupefilterPipeline' : 300
}

DUPEFILTER_PIPELINE_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}

DUPEFILTER_CLASS = "scrapy_dupefilter_util.ItemRequestDupeFilter"
REQUEST_DUPEFILTER_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}

in Item class:

key = True
nullable = True

"""

DUPEFILTER_PIPELINE_CONFIG = "DUPEFILTER_PIPELINE_CONFIG"
REQUEST_DUPEFILTER_CONFIG = "REQUEST_DUPEFILTER_CONFIG"


def extract_keys(cls, attr_name='key'):
    return [field for field, attr in cls.fields.items() if attr.get(attr_name)]


def get_item_dict(items):
    res = dict()
    for item in items:
        val = item.copy()
        item_cls = item['item']
        val['keys'] = item.get('keys') or extract_keys(item_cls)
        val['nullable_fields'] = item.get('nullable_fields') or extract_keys(item_cls, 'nullable')
        res[item_cls] = val
    return res


def check_settings(settings):
    var_str = ['MONGO_URI', 'MONGO_DATABASE']
    for var in var_str:
        setting = settings.get(var)
        if not setting:
            logging.error("{} not set in settings".format(var))
        else:
            logging.debug(f'in check_settings: {var} = {setting}')


class ItemRequestDupeFilter(RFPDupeFilter):
    def __init__(self, mongo_uri, mongo_db, config, path=None, debug=None):
        super(ItemRequestDupeFilter, self).__init__(path=path, debug=debug)
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.config = config
        self.items = get_item_dict(self.config.get('items'))
        self.client = None
        self.db = None

    def open(self):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close(self, reason):
        super(ItemRequestDupeFilter, self).close(reason=reason)
        self.client.close()

    def request_seen(self, request):
        fp_seen = super(ItemRequestDupeFilter, self).request_seen(request)
        if fp_seen:
            return fp_seen
        item = request.meta.get('item')
        cls_info_res = find_class(item, self.items)
        if not cls_info_res or not cls_info_res['keys']:
            return
        key_dict = {key: item.get(key) for key in cls_info_res['keys']}
        res = self.db[cls_info_res['collection']].find_one(key_dict)
        return res is not None

    @classmethod
    def from_settings(cls, settings):
        check_settings(settings)
        debug = settings.getbool('DUPEFILTER_DEBUG')
        config = settings.getdict('REQUEST_DUPEFILTER_CONFIG', {})
        mongo_uri = settings.get('MONGO_URI')
        mongo_db = settings.get('MONGO_DATABASE')
        return cls(mongo_uri, mongo_db, config, path=job_dir(settings), debug=debug)


class DupefilterPipeline(object):
    """
        apply to one kind of item only
    """

    def __init__(self, mongo_uri, mongo_db, config):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.config = config
        self.items = get_item_dict(self.config.get('items'))
        self.client = None
        self.db = None

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    @classmethod
    def from_crawler(cls, crawler):
        check_settings(crawler.settings)
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            config=crawler.settings.get('DUPEFILTER_PIPELINE_CONFIG')
        )

    def process_item(self, item, spider):
        for item_cls, cls_info in self.items.items():
            if isinstance(item, item_cls):
                cls_info_res = cls_info
                break
        else:   # non in responsible item classes
            return

        keys = cls_info_res['keys']
        key_dict = {key: item.get(key) for key in cls_info_res['keys']}
        collection_name = cls_info_res['collection']
        res = self.db[collection_name].find_one(key_dict)

        output = item.serializer() if hasattr(item, 'serializer') else dict(item)
        if not keys or not res:  # not exist
            self.db[collection_name].insert(output)
            spider.crawler.stats.inc_value('pipeline/mongodb_item_insert_cnt')
        else:
            set_dict = {key: item[key] for key in cls_info_res['nullable_fields']
                        if res.get(key) and not item.get(key, None)}
            if set_dict:
                self.db[collection_name].update(key_dict, {"$set": output})   # complete field
                spider.crawler.stats.inc_value('pipeline/mongodb_item_update_cnt')
        return item
