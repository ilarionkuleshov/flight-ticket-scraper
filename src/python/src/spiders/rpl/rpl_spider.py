import json
from furl import furl

from scrapy import Spider, Request
from rmq.utils import get_import_full_name
from pipelines import JsonPipeline


class RplSpider(Spider):
    name = "rpl"
    custom_settings = {"ITEM_PIPELINES": {get_import_full_name(JsonPipeline): 800,}}

    def start_requests(self):
        yield Request(
            url="https://biletyczarterowe.r.pl/api/wyszukiwanie/filtry/lokalizacje?czyDokad=false",
            callback=self.parse_locations_to
        )
        '''yield Request(
            url="https://biletyczarterowe.r.pl/api/destynacja/wyszukaj-wylot?iataDokad%5B%5D=TIA&dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30&dataMin=2022-09-01&dataMax=2023-09-01&oneWay=false",
            callback=self.parse_tickets
        )'''

    def parse_locations_to(self, response):
        locations_to_json = json.loads(response.text)
        for country in locations_to_json:
            for city in country["Miasta"]:
                search_furl = furl("https://biletyczarterowe.r.pl/api/destynacja/wyszukaj-wylot?iataDokad%5B%5D=&dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30&dataMin=&dataMax=&oneWay=false")
                search_furl.args["iataDokad[]"] = city["Klucz"]
                search_furl.args["dataMin"] = "2022-09-01"
                search_furl.args["dataMax"] = "2023-09-01"
                yield Request(
                    url=search_furl.url,
                    callback=self.parse_tickets
                )

    def parse_tickets(self, response):
        tickets_json = json.loads(response.text)
        for date_tickets in tickets_json:
            date = date_tickets["Data"]
            for ticket in date_tickets["Bilety"]:
                time_from = ticket["Wylot"]["Godzina"]
                time_to = ticket["Przylot"]["Godzina"]
                if time_from and time_to:
                    if response.meta.get("ticket_url"):
                        ticket_url = response.meta["ticket_url"]
                    else:
                        ticket_furl = furl("https://biletyczarterowe.r.pl/destynacja?data=&dokad%5B%5D=&idWylot=&oneWay=false&przylotDo&przylotOd&wiek%5B%5D=1989-10-30&wiek%5B%5D=1989-10-30&wylotDo&wylotOd")
                        ticket_furl.args["data"] = date
                        ticket_furl.args["dokad[]"] = ticket["Przylot"]["Iata"]
                        ticket_furl.args["idWylot"] = ticket["Id"]
                        ticket_url = ticket_furl.url

                    self.logger.info(f"Parsed new ticket {ticket_url}")
                    yield {
                        "url": ticket_url,
                        "flight_id": ticket["Id"],
                        "location_from": ticket["Wylot"]["Iata"],
                        "location_to": ticket["Przylot"]["Iata"],
                        "date": date,
                        "time_from": time_from,
                        "time_to": time_to,
                        "flight_time": ticket["CzasLotu"],
                        "baggage_small": ticket["BagazPodreczny"],
                        "baggage_big": ticket["BagazRejestrowany"],
                        "price": ticket["Cena"]
                    }
                    return_flights = ticket["Pakiety"]
                    if return_flights:
                        search_furl = furl("https://biletyczarterowe.r.pl/api/destynacja/wyszukaj-przylot?dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30")
                        for return_id in return_flights:
                            search_furl.args.add("pakietId[]", return_id)
                        yield Request(
                            url=search_furl.url,
                            callback=self.parse_tickets,
                            meta={"ticket_url": ticket_url}
                        )
