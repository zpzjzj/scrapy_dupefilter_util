from typing import Dict


def find_class(item, item_dict: Dict):
    for item_cls, cls_info in item_dict.items():
        if isinstance(item, item_cls):
            return cls_info
    else:
        return None


def get_item_dict(items, settings):
    def extract_keys(cls, attr_name='key'):
        return [field for field, attr in cls.fields.items() if attr.get(attr_name)]
    # set attributes for each item class
    res = dict()  # item cls => attributes (keys, nullable_fields, es_index, es_type, es_unique_keys ...)
    default_index = settings.get('ELASTICSEARCH_INDEX')
    default_type = settings.get('ELASTICSEARCH_TYPE')
    default_uniq_key = settings.get('ELASTICSEARCH_UNIQ_KEY')
    for item in items:
        val = item.copy()
        item_cls = item['item']
        val['keys'] = item.get('keys') or extract_keys(item_cls)
        val['nullable_fields'] = item.get('nullable_fields') or extract_keys(item_cls, 'nullable')
        val['ELASTICSEARCH_INDEX'] = item.get('ELASTICSEARCH_INDEX') or default_index
        val['ELASTICSEARCH_TYPE'] = item.get('ELASTICSEARCH_TYPE') or default_type
        val['ELASTICSEARCH_UNIQ_KEY'] = item.get('ELASTICSEARCH_UNIQ_KEY') or default_uniq_key
        res[item_cls] = val
    return res