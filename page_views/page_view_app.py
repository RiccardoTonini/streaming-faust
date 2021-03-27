import faust

from page_views.records import PageViewRecord
from page_views import settings


app = faust.App(
    settings.APP_NAME,
    broker=settings.DEFAULT_BROKER_HOSTNAME,
    topic_partitions=settings.DEFAULT_TOPIC_PARTITION,
)


# Defines the source topic to read the “page view” events from,
# and we specify that every value in this topic is of the PageView type.
def create_topic(paged_view_app, record_class):
    return paged_view_app.topic(
        settings.APP_NAME,
        value_type=record_class,
    )


# Tables are dictionary-like in-memory data structure that is distributed across the cluster,
# and partitioned by the dictionary key.
# They are backed by a Kafka changelog topic used for persistence and fault-tolerance.
# Define a Table to maintain the page view count.
# You cannot modify a table outside of a stream operation.
# This means that you can only mutate the table from within an async for event in stream: block.
# We require this to align the table’s partitions with the stream’s,
# and to ensure the source topic partitions are correctly rebalanced to a different worker upon failure,
# along with any necessary table partitions.
# This source-topic-event to table-modification-event requirement also ensures that
# producing to the changelog and committing messages from the source happen simultaneously.
def get_page_view_table(paged_view_app):
    return paged_view_app.Table(settings.APP_NAME, default=int)


# Now that we have defined our input stream (topic to read from),
# as well as a table to maintain counts,
# we define an agent reading each page view event coming into the stream
# "group_by()" is used to repartition the input stream by the page id.
# This is so that we maintain counts on each instance sharded by the page id.
# This way in the case of failure, when we move the processing of some partition to another node,
# the counts for that partition (hence, those page ids) also move together
@app.agent(create_topic(app, PageViewRecord))
async def count_page_views(views):
    page_views = get_page_view_table(app)
    async for view in views.group_by(PageViewRecord.id):
        page_views[view.id] += 1
