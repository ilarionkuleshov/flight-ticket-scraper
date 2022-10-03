from scrapy import Field
from rmq.items import RMQItem


class SerpUrlItem(RMQItem):
    url = Field()
