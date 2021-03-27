from faust import Record

# Let’s now define a model that each page view event from the stream deserializes into.
# The record is used for JSON dictionaries and describes fields much like the new dataclasses in Python 3.7


class PageViewRecord(Record):
    id: str
    user: str
