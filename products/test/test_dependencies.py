import pytest
from mock import Mock, patch, call

from nameko import config
from products.dependencies import Storage


@pytest.fixture
def storage(test_config):
    provider = Storage()
    provider.container = Mock(config=config)
    provider.setup()
    return provider.get_dependency({})


def test_get_fails_on_not_found(storage):
    with pytest.raises(storage.NotFound) as exc:
        storage.get(2)
    assert 'Product ID 2 does not exist' == exc.value.args[0]


def test_get(storage, products):
    product = storage.get('LZ129')
    assert 'LZ129' == product['id']
    assert 'LZ 129 Hindenburg' == product['title']
    assert 135 == product['maximum_speed']
    assert 50 == product['passenger_capacity']
    assert 11 == product['in_stock']


def test_list(storage, products):
    listed_products = storage.list()
    assert (
        products == sorted(list(listed_products), key=lambda x: x['id']))

def test_delete_product(storage, products):
    product_id = 'LZ129'
    # Assume storage.client refers to the Redis client within the Storage class
    with patch.object(storage.client, 'exists', return_value=True) as mock_exists, \
         patch.object(storage.client, 'delete', return_value=1) as mock_delete:

        # Call the delete method
        exclusion_response = storage.delete(product_id)

        # Assertions
        assert exclusion_response is True
        mock_exists.assert_called_once_with('products:LZ129')
        mock_delete.assert_called_once_with('products:LZ129')

        # Ensure the product is reported as non-existent after deletion
        mock_exists.return_value = False
        assert not storage.client.exists('products:LZ129')



def test_delete_product_fails_not_found(storage, products):
    product_id = 'product_id_not_exists'
    with pytest.raises(storage.NotFound) as exc:
        storage.delete(product_id)
    assert f'Product ID {product_id} does not exist' == exc.value.args[0]


def test_create(product, redis_client, storage):

    storage.create(product)

    stored_product = redis_client.hgetall('products:LZ127')

    assert product['id'] == stored_product[b'id'].decode('utf-8')
    assert product['title'] == stored_product[b'title'].decode('utf-8')
    assert product['maximum_speed'] == int(stored_product[b'maximum_speed'])
    assert product['passenger_capacity'] == (
        int(stored_product[b'passenger_capacity']))
    assert product['in_stock'] == int(stored_product[b'in_stock'])


def test_find_products_by_id(storage):
    # Mock storage.client.pipeline and storage._from_hash
    with patch.object(storage.client, 'pipeline', autospec=True) as mock_pipeline, \
            patch.object(storage, '_from_hash', autospec=True) as mock_from_hash:
        # Setup the mock for the pipeline
        pipeline_instance = Mock()
        mock_pipeline.return_value = pipeline_instance

        # Set up test data
        product_ids = ['id1', 'id2', 'id3']
        redis_data = [
            {'product_id': 'id1', 'title': 'The Odyssey', 'in_stock': '899'},
            None,  # Simulate a missing product
            {'product_id': 'id3', 'title': 'The Iliad', 'in_stock': '300'}
        ]

        # Setup mock behavior for the pipeline
        pipeline_instance.execute.return_value = redis_data

        # Copying the behaviour of the hgetall as a lambda method
        # It iterates for each item first and then return the next element of the result of the iteraction.
        # As the HGETALL can return NONE, it is necessary to check and filter for none elements.
        # None is returned IF all elements got filtered. That comes from next method.
        pipeline_instance.hgetall.side_effect = lambda key: next((item for item in redis_data if item), None)

        # Setup mock_from_hash to return the input if it's not None
        mock_from_hash.side_effect = lambda x: x if x else None

        # Call the method
        results = storage.find_products_by_id(product_ids)

        # Assertions
        assert len(results) == 2
        assert results[0]['product_id'] == 'id1'
        assert results[1]['product_id'] == 'id3'

        # Check calls to hgetall using call_args_list for ordered calls
        expected_calls = [call(storage._format_key(pid)) for pid in product_ids]
        assert pipeline_instance.hgetall.call_args_list == expected_calls


def test_decrement_stock(storage, create_product, redis_client):
    create_product(id=1, title='LZ 127', in_stock=10)
    create_product(id=2, title='LZ 129', in_stock=11)
    create_product(id=3, title='LZ 130', in_stock=12)

    in_stock = storage.decrement_stock(2, 4)

    assert 7 == in_stock
    product_one, product_two, product_three = [
        redis_client.hgetall('products:{}'.format(id_))
        for id_ in (1, 2, 3)]
    assert b'10' == product_one[b'in_stock']
    assert b'7' == product_two[b'in_stock']
    assert b'12' == product_three[b'in_stock']