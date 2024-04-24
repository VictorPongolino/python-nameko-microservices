from nameko import config
from nameko.extensions import DependencyProvider
import redis

from products.exceptions import NotFound


REDIS_URI_KEY = 'REDIS_URI'


class StorageWrapper:
    """
    Product storage

    A very simple example of a custom Nameko dependency. Simplified
    implementation of products database based on Redis key value store.
    Handling the product ID increments or keeping sorted sets of product
    names for ordering the products is out of the scope of this example.

    """

    NotFound = NotFound

    def __init__(self, client):
        self.client = client

    def _format_key(self, product_id):
        return f'products:{product_id}'

    def _from_hash(self, document):
        return {
            'id': document[b'id'].decode('utf-8'),
            'title': document[b'title'].decode('utf-8'),
            'passenger_capacity': int(document[b'passenger_capacity']),
            'maximum_speed': int(document[b'maximum_speed']),
            'in_stock': int(document[b'in_stock'])
        }

    def get(self, product_id):
        product = self.client.hgetall(self._format_key(product_id))
        if not product:
            raise NotFound(f'Product ID {product_id} does not exist')
        return self._from_hash(product)

    def list(self):
        cursor = '0'
        while cursor != 0:
            cursor, keys = self.client.scan(cursor=cursor, match=self._format_key('*'), count=10)
            for key in keys:
                yield self._from_hash(self.client.hgetall(key))

    def create(self, product):
        self.client.hmset(
            self._format_key(product['id']),
            product)

    def find_order_details_by_id(self, orders_id):
        """
            Retrieves product details for a set of product IDs.

            Parameters:
            - orders_id: A list of order details IDs.

            Returns:
            - list: A list of dictionaries containing product details.
        """
        pipeline = self.client.pipeline()
        keys = [self._format_key(order_id) for order_id in orders_id]
        for key in keys:
            pipeline.hgetall(key)
        results = pipeline.execute()

        # Filter out non-existing products and convert hash maps to dictionaries
        products = []
        for result in results:
            if result:
                products.append(self._from_hash(result))

        if not products:
            raise NotFound("No products found for the given IDs.")

        return products

    def delete(self, product_id):
        result = self.client.delete(self._format_key(product_id))
        if result == 0:
            raise NotFound(f'Product ID {product_id} does not exist')
        return result

    def decrement_stock(self, product_id, amount):
        return self.client.hincrby(
            self._format_key(product_id), 'in_stock', -amount)


class Storage(DependencyProvider):

    def setup(self):
        self.client = redis.StrictRedis.from_url(config.get(REDIS_URI_KEY))

    def get_dependency(self, worker_ctx):
        return StorageWrapper(self.client)
