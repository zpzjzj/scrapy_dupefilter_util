# scrapy 下 item 重复过滤器
scrapy 框架下，通过设置和检查 item 的特征项，避免重复写入数据库或重复爬取。

主要分为：
1. `ItemRequestDupeFilter` 请求过滤中间件，根据当前局部提取的 item 的特征项 (key) 查找是否已在数据库中存在，若有，则过滤掉该请求。
+ 存在目的：在爬虫爬取过程中，通过对爬虫请求中已抽取的局部特征项进行匹配检查是否已在数据库中存在，及时止损。适用于对临时链接的爬取，间隔时间较长的增量爬取。
2. `DupefilterPipeline` 导入到数据库的中间件，检查数据库中是否已有该 item 项，若有且无需更新，则不再加入到数据库中。
   + 存在目的：保证爬虫的多次运行在数据库中只留一份实例。


目前可用于 MongoDB, ElasticSearch


## 通用设置
在 scrapy 的 settings.py 或 爬虫代码中设置
```python
# 这里路径需要填补完全，否则会出现 item 和 ItemClass 类型不匹配的情况
from <project>.items import <item_class>

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'scrapy'

item = {'item': <item_class>, 'collection': <collection>}
```

Item 类中定义 item 对象的特征字段和待补全字段:

```python
class <item_class>(scrapy.Item):
	<key_field> = scrapy.Field(key = True)  # key 用于标识 item 的特征，用于数据库的查找
	<field> = scrapy.Field(nullable = True) # nullable 表明该字段可在初期为 null, 之后更新内容。用于由于反爬虫等原因导致初次爬取时数据不全的情况的更新补充。
```

也可以在上面的 setttings 中 item 项中定义
```python
item = {'item': <item_class>, 'collection': <collection>, 'keys': [<field1>, <field2>], 'nullable_fields': [<field>]}
```


## ItemRequestDupeFilter 设置 

```python
DUPEFILTER_CLASS = "scrapy_dupefilter_util.ItemRequestDupeFilter"
REQUEST_DUPEFILTER_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}
```



## DupefilterPipeline 设置 

```python
ITEM_PIPELINES = {
  'scrapy_dupefilter_util.DupefilterPipeline' : 300
}
DUPEFILTER_PIPELINE_CONFIG: {'items': [{'item': <item_class>, 'collection': <collection> }]}
```
### ElasticSearch 过滤
基于 [scrapy-elasticsearch](https://github.com/noplay/scrapy-elasticsearch) 建立对各个 item 项的单独配置支持和检查。

路径分别改为 `scrapy_dupefilter_util.es.ItemRequestDupeFilter` 和 `scrapy_dupefilter_util.es.DupefilterPipeline`

参数：

+ ELASTICSEARCH_INDEX
+ ELASTICSEARCH_TYPE
+ ELASTICSEARCH_UNIQ_KEY (optional) item 的特征字段，可用于 hash 生成唯一的文档 _id


参数可在全局的 settings 中设置，也可以对各个 item 项设置。如

```
item = {'item': <item_class>, 'collection': <collection>, 'keys': [<field1>, <field2>], 'nullable_fields': [<field>], 'ELASTICSEARCH_UNIQ_KEY': <uniq_key>}
DUPEFILTER_PIPELINE_CONFIG: {'items': [item]}
```



TODO:

[ ] `nullable_fields` 在请求过滤期间的检查

[ ] 代码重构，减少冗余重复

[ ] 统一 mongodb 过滤器中的参数，添加对全局默认参数的设置