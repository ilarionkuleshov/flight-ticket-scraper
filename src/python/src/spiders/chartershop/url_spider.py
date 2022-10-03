import json
from furl import furl

from scrapy import Spider
from scrapy.http import FormRequest
from scrapy.utils.project import get_project_settings

from rmq.utils import get_import_full_name
from rmq.pipelines import ItemProducerPipeline

from items import SerpUrlItem


class UrlSpider(Spider):
    name = "chartershop_url"
    custom_settings = {"ITEM_PIPELINES": {get_import_full_name(ItemProducerPipeline): 310,}}

    def __init__(self, *args, **kwargs):
        super(UrlSpider, self).__init__(*args, **kwargs)
        settings = get_project_settings()
        self.result_queue_name = settings.get("CHARTERSHOP_SERP_TASKS")

    def start_requests(self):
        yield FormRequest(
            url="https://chartershop.com.ua/index.php/?option=com_charters&view=charters&format=json&cmd=location-from",
            callback=self.parse_location_from
        )

    def parse_location_from(self, response):
        location_from_json = json.loads(response.text)
        for location in location_from_json:
            if location["country"] != "Ukraine":
                from_id = str(location["id"])
                yield FormRequest(
                    url="https://chartershop.com.ua/index.php/?option=com_charters&view=charters&format=json&cmd=location-to",
                    callback=self.parse_location_to,
                    formdata={"from": from_id},
                    meta={"from": from_id}
                )

    def parse_location_to(self, response):
        location_to_json = json.loads(response.text)
        from_id = response.meta.get("from")
        for location in location_to_json:
            if location["country"] != "Ukraine":
                to_id = str(location["id"])
                yield FormRequest(
                    url="https://chartershop.com.ua/index.php/?option=com_charters&view=charters&format=json&cmd=departure-dates",
                    callback=self.parse_departure_dates,
                    formdata={"from": from_id, "to": to_id},
                    meta={"from": from_id, "to": to_id}
                )
                yield FormRequest(
                    url="https://chartershop.com.ua/index.php/?option=com_charters&view=charters&format=json&cmd=departure-dates",
                    callback=self.parse_departure_dates,
                    formdata={"from": to_id, "to": from_id},
                    meta={"from": to_id, "to": from_id}
                )

    def parse_departure_dates(self, response):
        departure_dates_json = json.loads(response.text)
        for departure in departure_dates_json:
            serp_furl = furl("https://chartershop.com.ua/search.html")
            serp_furl.args["from"] = response.meta.get("from")
            serp_furl.args["to"] = response.meta.get("to")
            serp_furl.args["departure"] = departure
            serp_furl.args["adults"] = "1"
            serp_furl.args["children"] = "0"
            serp_furl.args["infants"] = "0"
            self.logger.info(f"Parsed new serp url {serp_furl.url}")
            yield SerpUrlItem({"url": serp_furl.url})
