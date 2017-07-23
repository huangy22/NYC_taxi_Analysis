import fiona
from shapely.geometry import shape
from shapely.ops import cascaded_union, unary_union

polygons =[shape(feature['geometry') for feature in fiona.open("./taxi_zones/taxi_zones_new.shp")]
union_poly = cascaded_union(polygons)
