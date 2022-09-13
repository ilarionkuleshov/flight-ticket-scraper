import re
import json
from datetime import datetime, timedelta
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
                    callback=self.parse_return_flights
                )

    def parse_return_flights(self, response):
        flights_json = json.loads(response.text)
        for date_tickets in flights_json:
            for ticket in date_tickets["Bilety"]:
                pakiety = ticket["Pakiety"]
                if pakiety:
                    search_furl = furl("https://biletyczarterowe.r.pl/api/destynacja/wyszukaj-przylot?dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30")
                    for pakiet in pakiety:
                        search_furl.args.add("pakietId[]", pakiet)
                    yield Request(
                        url=search_furl.url,
                        callback=self.parse_bind_flights
                    )

    def parse_bind_flights(self, response):
        flights_json = json.loads(response.text)
        for date_tickets in flights_json:
            for ticket in date_tickets["Bilety"]:
                tickets_furl = furl("https://biletyczarterowe.r.pl/api/zamowienie/pobierz-bilety?pakietIdWylot=&pakietIdPrzylot=&dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30")
                tickets_furl.args["pakietIdWylot"] = ticket["BindId"]
                tickets_furl.args["pakietIdPrzylot"] = ticket["Id"]
                yield Request(
                    url=tickets_furl.url,
                    callback=self.parse_tickets
                )

    def parse_tickets(self, response):
        tickets_json = json.loads(response.text)
        try:
            ticket_from = self.get_ticket_info(tickets_json["Wylot"])
            ticket_to = self.get_ticket_info(tickets_json["Przylot"])

            if ticket_from is not None and ticket_to is not None:
                tickets_furl = furl("https://biletyczarterowe.r.pl/formularz?oneWay=false&pakietIdPrzylot=&pakietIdWylot=&wiek%5B%5D=1989-10-30&wiek%5B%5D=1989-10-30")
                tickets_furl.args["pakietIdWylot"] = ticket_from["flight_id"]
                tickets_furl.args["pakietIdPrzylot"] = ticket_to["flight_id"]
                tickets_url = tickets_furl.url

                ticket_from["url"] = tickets_url
                ticket_to["url"] = tickets_url

                self.logger.info(f"Parsed tickets {tickets_url}")
                yield ticket_from
                yield ticket_to

        except Exception as e:
            self.logger.error(f"{e} {response.url}")

    def get_ticket_info(self, ticket_json):
        flight_number = ticket_json["NumerLotu"]
        date_from = ticket_json["DataWylotu"]
        time_from = ticket_json["Wylot"]["Godzina"]
        time_to = ticket_json["Przylot"]["Godzina"]
        flight_time = ticket_json["CzasLotu"]

        if flight_number is None or date_from is None or time_from is None or time_to is None or flight_time is None:
            return None

        flight_number_1, flight_number_2 = self.get_flight_numbers(flight_number)
        if flight_number_1 is None or flight_number_2 is None:
            print(flight_number)

        return {
            "flight_id": ticket_json["ID"],
            "company": ticket_json["NazwaPrzewoznika"],
            "flight_number_1": flight_number_1,
            "flight_number_2": flight_number_2,
            "location_from": ticket_json["Wylot"]["Iata"],
            "location_to": ticket_json["Przylot"]["Iata"],
            "date_from": date_from,
            "date_to": self.get_date_to(date_from, time_from, flight_time),
            "time_from": time_from,
            "time_to": time_to,
            "flight_time": flight_time,
            "baggage_small": ticket_json["BagazPodreczny"],
            "baggage_big": ticket_json["BagazRejestrowany"],
            "price": ticket_json["Cena"]
        }

    def get_flight_numbers(self, flight_number):
        last_character_index = flight_number.rfind("".join(re.findall("[a-zA-Z]+", flight_number))[-1])
        return flight_number[:last_character_index+1], flight_number[last_character_index+1:]

    def get_date_to(self, date_from, time_from, flight_time):
        dt_from = datetime.strptime(f"{date_from} {time_from}", "%Y-%m-%d %H:%M")
        dt_delta = datetime.strptime(flight_time, "%H:%M")
        dt_to = dt_from + timedelta(hours=dt_delta.hour, minutes=dt_delta.minute)
        return dt_to.strftime("%Y-%m-%d")
