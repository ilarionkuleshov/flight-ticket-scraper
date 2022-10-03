import json
from datetime import datetime as dt
from itemadapter import ItemAdapter


class JsonPipeline:
    def __init__(self):
        self.file_path = f"data/{dt.now().strftime('%d_%m_%Y_%H_%M_%S')}.jl"

    def process_item(self, item, spider):
        with open(self.file_path, "a") as file:
            line = json.dumps(ItemAdapter(item).asdict()) + "\n"
            file.write(line)
        return item
