import geonamescache


def city_exists(city_name):
    gc = geonamescache.GeonamesCache(min_city_population=15000)
    cities = gc.get_cities()
    return(len(gc.get_cities_by_name(city_name)) > 0)



print(city_exists('The Shire'))