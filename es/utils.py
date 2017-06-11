from scrapy.dupefilters import RFPDupeFilter
from scrapy.utils.job import job_dir
import logging
from scrapyelasticsearch.scrapyelasticsearch import ElasticSearchPipeline
from ..common.utils import find_class, get_item_dict

logger = logging.getLogger(__name__)

"""
SETTINGS

# 这里路径需要填补完全，否则会出现 item 和 ItemClass 类型不匹配的情况
from {project}.items import {ItemClass}

ELASTICSEARCH_SERVERS = ['http://localhost:9200']
ELASTICSEARCH_INDEX = 'domain'
ELASTICSEARCH_UNIQ_KEY = None

ITEM_PIPELINES = {
  'scrapy_dupefilter_util.es.DupefilterPipeline' : 300
}

DUPEFILTER_PIPELINE_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}

DUPEFILTER_CLASS = "scrapy_dupefilter_util.es.ItemRequestDupeFilter"
REQUEST_DUPEFILTER_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}

in Item class:

key = True
nullable = True

"""

DUPEFILTER_PIPELINE_CONFIG = "DUPEFILTER_PIPELINE_CONFIG"
REQUEST_DUPEFILTER_CONFIG = "REQUEST_DUPEFILTER_CONFIG"


def check_settings(settings):
    var_str = ['MONGO_URI', 'MONGO_DATABASE']
    for var in var_str:
        setting = settings.get(var)
        if not setting:
            logging.error("{} not set in settings".format(var))
        else:
            logging.debug(f'in check_settings: {var} = {setting}')


def search_item(es, item, cls_info_res):
    keys = cls_info_res['keys']
    index = cls_info_res.get('ELASTICSEARCH_INDEX')
    type_name = cls_info_res.get('ELASTICSEARCH_TYPE')
    if not es.indices.exists_type(index, type_name):
        return None
    query = {
        "query": {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"match": {key: item.get(key)}} for key in keys]
                    }
                }
            }
        }
    }
    res = es.search(index=index, doc_type=type_name, body=query)
    if res['hits']['total'] <= 0:
        return None
    else:
        item = res['hits']['hits'][0]['_source']
        item['_id'] = res['hits']['hits'][0]['_id']
        return item


class ItemRequestDupeFilter(RFPDupeFilter):
    settings = None
    es = None
    items = dict()

    def request_seen(self, request):
        fp_seen = super(ItemRequestDupeFilter, self).request_seen(request)
        if fp_seen:
            return fp_seen
        item = request.meta.get('item')

        for item_cls, cls_info in self.items.items():
            if isinstance(item, item_cls) and cls_info['keys']:
                cls_info_res = cls_info
                break
        else:
            return
        keys = cls_info_res['keys']
        key_dict = {key: item.get(key) for key in keys}
        if keys and key_dict:
            res = search_item(self.es, item, cls_info_res)
            return res is not None

    @classmethod
    def from_settings(cls, settings):
        from elasticsearch import Elasticsearch
        check_settings(settings)
        debug = settings.getbool('DUPEFILTER_DEBUG')
        config = settings.getdict('REQUEST_DUPEFILTER_CONFIG', {})
        obj = cls(path=job_dir(settings), debug=debug)
        obj.settings = settings
        es_servers = obj.settings['ELASTICSEARCH_SERVERS']
        es_servers = es_servers if isinstance(es_servers, list) else [es_servers]
        obj.items = get_item_dict(config.get('items'), settings)
        obj.es = Elasticsearch(hosts=es_servers, timeout=obj.settings.get('ELASTICSEARCH_TIMEOUT', 60))
        return obj


class DupefilterPipeline(ElasticSearchPipeline):
    """
        apply to one kind of item only
    """
    items = dict()

    @classmethod
    def from_crawler(cls, crawler):
        ext = super(DupefilterPipeline, DupefilterPipeline).from_crawler(crawler)
        check_settings(crawler.settings)
        ext.items = get_item_dict(crawler.settings.get('DUPEFILTER_PIPELINE_CONFIG').get('items'), crawler.settings)
        return ext

    def search_item(self, item, cls_info_res):
        keys = cls_info_res['keys']
        index = cls_info_res.get('ELASTICSEARCH_INDEX')
        type_name = cls_info_res.get('ELASTICSEARCH_TYPE')
        if not self.es.indices.exists_type(index, type_name):
            return None
        query = {
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [{"match": {key: item.get(key)}} for key in keys]
                        }
                    }
                }
            }
        }
        res = self.es.search(index=index, doc_type=type_name, body=query)
        if res['hits']['total'] <= 0:
            return None
        else:
            item = res['hits']['hits'][0]['_source']
            item['_id'] = res['hits']['hits'][0]['_id']
            return item

    def bulk_item(self, item, cls_info_res, operation='index'):
        is_index = operation == 'index'
        index_name = cls_info_res.get('ELASTICSEARCH_INDEX')
        index_action = {
            '_index': index_name,
            '_type': cls_info_res.get('ELASTICSEARCH_TYPE'),
            '_source' if is_index else 'doc': item,
            '_op_type': operation,  # python 库的接口方式和 api 不一致，angry！
        }
        unique_key = cls_info_res.get('ELASTICSEARCH_UNIQ_KEY') or self.settings['ELASTICSEARCH_UNIQ_KEY']
        if is_index and unique_key is not None:
            item_unique_key = item[self.settings['ELASTICSEARCH_UNIQ_KEY']]
            unique_key = self.get_unique_key(item_unique_key)
            import hashlib
            item_id = hashlib.sha1(unique_key).hexdigest()
            index_action['_id'] = item_id
            logging.debug('Generated unique key %s' % item_id)
        else:
            index_action['_id'] = item.pop('_id')

        self.items_buffer.append(index_action)
        # print(str(self.items_buffer).replace('\'', '\"'))
        self.send_items()
        if len(self.items_buffer) >= self.settings.get('ELASTICSEARCH_BUFFER_LENGTH', 500):
            self.send_items()
            self.items_buffer = []

    def process_item(self, item, spider):
        import types
        if isinstance(item, types.GeneratorType) or isinstance(item, list):
            for each in item:
                self.process_item(each, spider)
        else:
            cls_info_res = find_class(item, self.items)
            if not cls_info_res:
                return
            keys = cls_info_res['keys']
            res = self.search_item(item, cls_info_res)
            if not keys or not res:  # no keys for checking or not exists
                output = item.serializer() if hasattr(item, 'serializer') else dict(item)
                spider.crawler.stats.inc_value('pipeline/es_item_insert_cnt')
                self.bulk_item(output, cls_info_res)
            else:
                set_dict = {key: item[key] for key in cls_info_res['nullable_fields']
                            if res.get(key) and not item.get(key, None)}
                if set_dict:
                    self.bulk_item({'_id': res.get('_id'), **set_dict}, cls_info_res, 'update')
                    spider.crawler.stats.inc_value('pipeline/es_item_update_cnt')
            return item
