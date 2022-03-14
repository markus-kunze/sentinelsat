import datetime
from enum import Enum
from typing import List


class NameQueryFunction(Enum):
    CONTAINS = "contains"
    ENDSWITH = "endswith"
    STARTSWITH = "startswith"


class NameQuery:
    def __init__(self, name: str = None, name_query_function: NameQueryFunction = None, query: str = ""):
        self.name = name
        self.name_query_function = name_query_function

        if query == "" and name is not None and name_query_function is not None:
            self.query = str("{}(Name,'{}')".format(name_query_function.value, name))
        else:
            self.query = query


class PublicationDateQueryParam:
    publication_date = "PublicationDate"
    greater_than = "gt"
    lower_than = "lt"
    and_ = "and"

    @classmethod
    def date(cls, param: datetime):
        return param.isoformat(sep='T', timespec='milliseconds') + "Z"


class PublicationDateQuery:
    def __init__(self, query: list):
        if isinstance(query, list):
            self.query = self.build(query)
        elif isinstance(query, str):
            self.query = query
        else:
            raise TypeError("Only list or str are supported as PublicationDateQuery input")

    def build(self, params: List[PublicationDateQueryParam]):
        query = ""
        for count, ele in enumerate(params):
            if count == 0:
                query += ele
            else:
                query += " {}".format(ele)
        return query


class SensingDateQueryParam:
    sensing_date_start = "ContentDate/Start"
    sensing_date_stop = "ContentDate/End"
    greater_than = "gt"
    lower_than = "lt"
    and_ = "and"

    @classmethod
    def date(cls, param: datetime):
        return param.isoformat(sep='T', timespec='milliseconds') + "Z"


class SensingDateQuery:
    def __init__(self, query):
        if isinstance(query, list):
            self.query = self.build(query)
        elif isinstance(query, str):
            self.query = query
        else:
            raise TypeError("Only list or string are supported as SensingDateQuery input")

    def build(self, params: List[SensingDateQueryParam]):
        query = ""
        for count, ele in enumerate(params):
            if count == 0:
                query += ele
            else:
                query += " {}".format(ele)
        return query


class AreaQuery:
    def __init__(self, query: str):
        if query == "":
            self.query = ""
        else:
            self.query = "OData.CSC.Intersects(area=geography'SRID=4326;{}')".format(query)
