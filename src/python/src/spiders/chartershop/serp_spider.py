import re
import json

from scrapy import Request
from scrapy.utils.project import get_project_settings
from scrapy.core.downloader.handlers.http11 import TunnelError

from rmq.spiders import TaskToMultipleResultsSpider
from rmq.utils.decorators import rmq_callback, rmq_errback
from rmq.utils import get_import_full_name
from pipelines import JsonPipeline


class SerpSpider(TaskToMultipleResultsSpider):
    name = "chartershop_serp"
    custom_settings = {"ITEM_PIPELINES": {get_import_full_name(JsonPipeline): 800,}}

    def __init__(self, *args, **kwargs):
        super(SerpSpider, self).__init__(*args, **kwargs)
        settings = get_project_settings()
        self.task_queue_name = settings.get("CHARTERSHOP_SERP_TASKS")

    def next_request(self, _delivery_tag, msg_body):
        data = json.loads(msg_body)
        return Request(data["url"], callback=self.parse, errback=self._errback)

    @rmq_callback
    def parse(self, response):
        ticket_selectors = response.xpath("//div[@class='page-results__tickets-list tickets-list']/div[@class='ticket']")
        for ticket in ticket_selectors:
            company = ticket.xpath(".//img[@class='ticket__company-logo']/@alt").get(default="")
            flight_name = ticket.xpath(".//div[@class='ticket__flight-name']/text()").get(default="").split(" ")
            if len(flight_name) == 2:
                flight_name_1 = flight_name[0]
                flight_name_2 = flight_name[1]
            else:
                flight_name_1 = ""
                flight_name_2 = ""
            company_text = ticket.xpath(".//div[@class='ticket__company-text']/text()").get(default="")

            route_cols = ticket.xpath(".//div[@class='ticket__route']/div[@class='ticket__route-col']")

            location_from, date_from, time_from = self.get_route_info(route_cols[0])
            location_to, date_to, time_to = self.get_route_info(route_cols[1])

            baggage_small = ticket.xpath(".//div[contains(@class, '_baggage-small')]/div/span/text()").get(default="")
            baggage_big = ticket.xpath(".//div[contains(@class, '_baggage-big')]/div/span/text()").get(default="")

            price = ticket.xpath(".//div[@class='ticket__main-price-val']/text()").get(default="")
            if price:
                price = re.sub("[^0-9]", "", price)

            seats = ticket.xpath(".//div[@class='ticket__side-notify']/text()").getall()
            if len(seats):
                seats = "".join(seats)
                seats = re.sub("[^0-9]", "", seats)
            else:
                seats = ""

            self.logger.info(f"Parsed serp page {response.url}")
            yield {
                "url": response.url,
                "company": company,
                "flight_name_1": flight_name_1,
                "flight_name_2": flight_name_2,
                "company_text": company_text,
                "location_from": location_from,
                "date_from": date_from,
                "time_from": time_from,
                "location_to": location_to,
                "date_to": date_to,
                "time_to": time_to,
                "baggage_small": baggage_small,
                "baggage_big": baggage_big,
                "price": price,
                "seats": seats
            }

    def get_route_info(self, selector):
        location = selector.xpath(".//div[@class='ticket__route-name-abr']/text()").get(default="")
        date = selector.xpath(".//div[@class='ticket__route-date']/text()").get(default="")
        time = selector.xpath(".//div[@class='ticket__route-time']/text()").get(default="")
        return location.strip(), date.strip(), time.strip()

    @rmq_errback
    def _errback(self, failure):
        if failure.check(TunnelError):
            self.logger.info("TunnelError. Copy request")
            yield failure.request.copy()
        else:
            self.logger.warning(f"IN ERRBACK: {repr(failure)}")
