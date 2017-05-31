# 过滤重复 item 的中间件
主要分为：
1. `ItemRequestDupeFilter` 请求过滤中间件，根据当前局部提取的 item 的特征项 (key) 查找是否已在数据库中存在，若有，则过滤掉该请求。

2. `DupefilterPipeline` 导入到数据库的中间件，检查数据库中是否已有该 item 项，若有且无需更新，则不再加入到数据库中。


目前用于 MongoDB (**TODO**：适配 Elastic Search)


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
	<key_field> = scrapy.Field(key = True) # key 用于标识 item 的特征，用于数据库的查找
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