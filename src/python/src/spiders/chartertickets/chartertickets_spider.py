import re
import json
from datetime import datetime, timedelta
from furl import furl

from scrapy import Spider, Request
from scrapy.http import FormRequest

from rmq.utils import get_import_full_name
from pipelines import JsonPipeline


class CharterticketsSpider(Spider):
    name = "chartertickets"
    custom_settings = {"ITEM_PIPELINES": {get_import_full_name(JsonPipeline): 800,}}

    def start_requests(self):
        yield Request(
            url="https://chartertickets.com.ua/?type=one_way",
            callback=self.parse_locations
        )

    def parse_locations(self, response):
        init_script = response.xpath("//script[contains(text(), 'searchPage.init')]/text()").get()
        if init_script:
            init_script = init_script.split("searchPage.init('', '[")[1].split("]'")[0]
            init_script = f"[{init_script}]".encode().decode("unicode_escape")
            init_json = json.loads(init_script)
            for locations in init_json:
                city_from, city_to = locations.split(";")
                date_from_furl = furl("https://chartertickets.com.ua/api/get-date-departure-tickets?type=one_way")
                date_from_furl.args["city_from"] = city_from
                date_from_furl.args["city_to"] = city_to
                yield Request(
                    url=date_from_furl.url,
                    callback=self.parse_date_from,
                    meta={
                        "city_from": city_from,
                        "city_to": city_to
                    }
                )
        else:
            self.logger.error("Failed to find init script on page")

    def parse_date_from(self, response):
        response_json = json.loads(response.text)
        if "data" in response_json:
            for date in response_json["data"]:
                yield FormRequest(
                    url="https://chartertickets.com.ua/api/get-flight-online",
                    callback=self.parse_tickets,
                    formdata={
                        "departureCityId": response.meta.get("city_from"),
                        "destinationCityId": response.meta.get("city_to"),
                        "OutgoingDepartureDates": date,
                        "ReturnDepartureDates": "",
                        "adults": "1",
                        "children": "0",
                        "infants": "0",
                        "type": "one_way"
                    }
                )
        else:
            self.logger.error(f"Failed to find dates from: {response.url}")

    def parse_tickets(self, response):
        response_json = json.loads(response.text)
        for ticket in response_json:
            flight_names = ticket["FreightName"].split(" ")
            time_from = ticket["SrcTime"].split(", ")
            time_to = ticket["TrgTime"].split(", ")

            self.logger.info(f"Parsed new ticket from page {response.url}")
            yield {
                "url": response.url,
                "company": ticket["PartnerInName"],
                "flight_name_1": flight_names[0],
                "flight_name_2": flight_names[1],
                "location_from": ticket["SrcPortName"],
                "date_from": time_from[1],
                "time_from": time_from[0],
                "location_to": ticket["TrgPortName"],
                "date_to": time_to[1],
                "time_to": time_to[0],
                "baggage_desc": ticket["BaggageDesc"],
                "price_uah": ticket["price"],
                "price_usd": ticket["price_usd"],
                "seats": "1"
            }
