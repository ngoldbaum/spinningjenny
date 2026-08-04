from spinningjenny import ThreadPoolExecutor


def test_minimal():
    with ThreadPoolExecutor(2) as executor:
        assert sorted(executor.map_unordered(lambda x: x * 2, range(3))) == [0, 2, 4]
