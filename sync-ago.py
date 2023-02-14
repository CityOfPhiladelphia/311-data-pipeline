import os, sys
import pytz
import pandas as pd
from arcgis import GIS
from arcgis.features import FeatureLayerCollection
import datetime
import cx_Oracle
from pprint import pprint
from time import sleep
import petl as etl
import pyproj
import shapely.wkt
from shapely.ops import transform as shapely_transformer
from config import *
import click


@click.command()
@click.option('--day', '-d', help='Retrieve and update records that were updated on a specific day (e.g. 2016-05-18). This is mostly for debugging and maintenance purposes.')
def sync(day):
    # They're the same for saleforce so we should need no projecting of points.
    # Hardcode this to make the code work, but we can modularize this lter.
    IN_SRID = 4326
    AGO_SRID = 4326

    PRIMARY_KEY = 'SERVICE_REQUEST_ID'

    '''transformer needs to be defined outside of our row loop to speed up projections.'''
    TRANSFORMER = pyproj.Transformer.from_crs(f'epsg:{IN_SRID}',
                                               f'epsg:{AGO_SRID}',
                                               always_xy=True)

    print('Connecting to AGO...')
    org = GIS(url='https://phl.maps.arcgis.com',
                username='maps.phl.data',
                password=MAPS_PASSWORD,
                verify_cert=False)
    print('Connected to AGO.\n')

    flayer = org.content.get(SALESFORCE_AGO_ITEMID)
    LAYER_OBJECT = flayer.layers[0]
    print(LAYER_OBJECT)

    GEOMETRIC = LAYER_OBJECT.properties.geometryType

    if GEOMETRIC:
        # self._geometric = True
        print(f'Item detected as geometric, type: {GEOMETRIC}\n')
    else:
        raise AssertionError('Item is not geometric.\n')


    def database_connect():
        user = 'sde'
        password = DATABRIDGE_SDE_PASSWORD
        host = DATABRIDGE_HOST
        service_name = 'GISDBP'
        port = 1521
        dsn = cx_Oracle.makedsn(host, port, service_name)
        # Connect to database
        db_connect = cx_Oracle.connect(user, password, dsn, encoding="UTF-8")
        print('Connected to %s' % db_connect)
        cursor = db_connect.cursor()
        return cursor

    cursor = database_connect()
    print("Connected to Oracle!\n")


    def project_and_format_shape(wkt_shape):
        ''' Helper function to help format spatial fields properly for AGO '''
        # Note: list of coordinates for polygons are called "rings" for some reason
        def format_ring(poly):
            if IN_SRID != AGO_SRID:
                transformed = shapely_transformer(TRANSFORMER.transform, poly)
                xlist = list(transformed.exterior.xy[0])
                ylist = list(transformed.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(poly.exterior.xy[0])
                ylist = list(poly.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        def format_path(line):
            if IN_SRID != AGO_SRID:
                transformed = shapely_transformer(TRANSFORMER.transform, line)
                xlist = list(transformed.coords.xy[0])
                ylist = list(transformed.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(line.coords.xy[0])
                ylist = list(line.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        if 'POINT' in wkt_shape:
            pt = shapely.wkt.loads(wkt_shape)
            if IN_SRID != AGO_SRID:
                x, y = TRANSFORMER.transform(pt.x, pt.y)
                return x, y
            else:
                return pt.x, pt.y
        elif 'MULTIPOLYGON' in wkt_shape:
            multipoly = shapely.wkt.loads(wkt_shape)
            assert multipoly.is_valid
            list_of_rings = []
            for poly in multipoly:
                assert poly.is_valid
                # reference for polygon projection: https://gis.stackexchange.com/a/328642
                ring = format_ring(poly)
                list_of_rings.append(ring)
            return list_of_rings
        elif 'POLYGON' in wkt_shape:
            poly = shapely.wkt.loads(wkt_shape)
            assert poly.is_valid
            ring = format_ring(poly)
            return ring
        elif 'LINESTRING' in wkt_shape:
            path = shapely.wkt.loads(wkt_shape)
            path = format_path(path)
            return path
        else:
            raise NotImplementedError('Shape unrecognized.')


    def return_coords_only(self,wkt_shape):
        ''' Do not perform projection, simply extract and return our coords lists.'''
        poly = shapely.wkt.loads(wkt_shape)
        return poly.exterior.xy[0], poly.exterior.xy[1]


    def format_row(row):
        clean_columns = ['description', 'status_notes']
        # Clean our designated row of non-utf-8 characters or other undesirables that makes AGO mad.
        # If you pass multiple values separated by a comma, it will perform on multiple colmns
        for column in clean_columns:
            if row[column] == None:
                pass
            else:
                row[column] = row[column].encode("ascii", "ignore").decode()
                row[column] = row[column].replace('\'', '')
                row[column] = row[column].replace('"', '')
                row[column] = row[column].replace('<', '')
                row[column] = row[column].replace('>', '')

        # Convert cx_Oracle.LOB to simple string via the read method
        # Source: https://stackoverflow.com/a/12590977
        new_row['shape'] = new_row['shape'].read()

        # Convert None values to empty string
        # but don't convert date fields to empty strings,
        # Apparently arcgis API needs a None value to properly pass a value as 'null' to ago.
        for col in row.keys():
            if row[col] == None:
                if 'datetime' not in col:
                    row[col] = ''
        for col in row.keys():
            if 'datetime' in col and row[col] == '':
                row[col] = None
        # Check to make sure rows aren't incorrectly set as UTC. Convert to EST/EDT if so.
            if row[col]:
                if 'datetime' in col and '+0000' in row[col]:
                    dt_obj = datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S %z")
                    local_dt_obj = obj.astimezone(pytz.timezone('US/Eastern'))
                    row[col] = local_db_obj.strftime("%Y-%m-%d %H:%M:%S %z")

        # remove the shape field so we can replace it with SHAPE with the spatial reference key
        # and also store in 'wkt' var (well known text) so we can project it
        wkt = row.pop('shape')

        # Oracle sde.st_astext() function returns empty geometry as this string
        # Set to empty string so the next conditional works.
        if wkt == 'POINT EMPTY':
            wkt = ''

        # If the geometry cell is blank, properly pass a NaN or empty value to indicate so.
        if not (bool(wkt.strip())):
            if GEOMETRIC == 'esriGeometryPoint':
                geom_dict = {"x": 'NaN',
                             "y": 'NaN',
                             "spatial_reference": {"wkid": AGO_SRID}
                             }
                row_to_append = {"attributes": row,
                                "geometry": geom_dict
                                }
            elif GEOMETRIC == 'esriGeometryPolyline':
                geom_dict = {"paths": [],
                             "spatial_reference": {"wkid": AGO_SRID}
                             }
                row_to_append = {"attributes": row,
                                 "geometry": geom_dict
                                 }
            elif GEOMETRIC == 'esriGeometryPolygon':
                geom_dict = {"rings": [],
                             "spatial_reference": {"wkid": AGO_SRID}
                             }
                row_to_append = {"attributes": row,
                                 "geometry": geom_dict
                                 }
            else:
                raise TypeError(f'Unexpected geomtry type!: {GEOMETRIC}')
        # For different types we can consult this for the proper json format:
        # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
        if 'POINT' in wkt:
            projected_x, projected_y = project_and_format_shape(wkt)
                           # Format our row, following the docs on this one, see section "In [18]":
            # https://developers.arcgis.com/python/sample-notebooks/updating-features-in-a-feature-layer/
            # create our formatted point geometry
            geom_dict = {"x": projected_x,
                         "y": projected_y,
                         "spatial_reference": {"wkid": AGO_SRID}
                         }
            row_to_append = {"attributes": row,
                             "geometry": geom_dict}
        elif 'MULTIPOINT' in wkt:
            raise NotImplementedError("MULTIPOINTs not implemented yet..")
        elif 'MULTIPOLYGON' in wkt:
            rings = project_and_format_shape(wkt)
            geom_dict = {"rings": rings,
                         "spatial_reference": {"wkid": AGO_SRID}
                         }
            row_to_append = {"attributes": row,
                             "geometry": geom_dict
                             }
        elif 'POLYGON' in wkt:
            #xlist, ylist = return_coords_only(wkt)
            ring = project_and_format_shape(wkt)
            geom_dict = {"rings": [ring],
                         "spatial_reference": {"wkid": AGO_SRID}
                         }
            row_to_append = {"attributes": row,
                             "geometry": geom_dict
                             }
        elif 'LINESTRING' in wkt:
            paths = project_and_format_shape(wkt)
            geom_dict = {"paths": [paths],
                         "spatial_reference": {"wkid": AGO_SRID}
                         }
            row_to_append = {"attributes": row,
                             "geometry": geom_dict
                             }
        return row_to_append


    def edit_features(row, method='adds'):
        '''
        Complicated function to wrap the edit_features arcgis function so we can handle AGO failing
        It will handle either:
        1. A reported rollback from AGO (1003) and try one more time,
        2. An AGO timeout, which can still be successful which we'll verify with a row count.
        '''

        def is_rolled_back(result):
            '''
            If we receieve a vague object back from AGO and it contains an error code of 1003
            docs:
            https://community.esri.com/t5/arcgis-api-for-python-questions/how-can-i-test-if-there-was-a-rollback/td-p/1057433
            ESRi lacks documentation here for us to really know what to expect..
            '''
            if result is None:
                print('Returned result object is None? In cases like this the append seems to fail completely, possibly from bad encoding. Retrying.')
                # print(f'Example row from this batch: {adds[0]}')
                print(f'batch: {row}')
                print(f'Returned object: {pprint(result)}')
                return True
            elif result["addResults"] is None:
                print('Returned result not what we expected, assuming success.')
                print(f'Returned object: {pprint(result)}')
                return False
            elif result["addResults"] is not None:
                for element in result["addResults"]:
                    if "error" in element and element["error"]["code"] == 1003:
                        return True
                    elif "error" in element and element["error"]["code"] != 1003:
                        raise Exception(f'Got this error returned from AGO (unhandled error): {element["error"]}')
                return False

        success = False
        # save our result outside the while loop
        result = None
        tries = 0
        while success is False:
            tries += 1
            if tries > 5:
                raise Exception(
                    'Too many retries on this batch, there is probably something wrong with a row in here! Giving up!')
                # break
            # Is it still rolled back after a retry?
            if result is not None:
                if is_rolled_back(result):
                    raise Exception("Retry on rollback didn't work.")

            # Add the batch
            try:
                if method == "adds":
                    result = LAYER_OBJECT.edit_features(adds=row, rollback_on_failure=True)
                elif method == "updates":
                    result = LAYER_OBJECT.edit_features(updates=row, rollback_on_failure=True)
                elif method == "deletes":
                    result = LAYER_OBJECT.edit_features(deletes=row, rollback_on_failure=True)
            except Exception as e:
                if '504' in str(e):
                    # let's try ignoring timeouts for now, it seems the count catches up eventually
                    continue

            if is_rolled_back(result):
                print("Results rolled back, retrying our batch adds in 15 seconds....")
                sleep(15)
                try:
                    if method == "adds":
                        result = LAYER_OBJECT.edit_features(adds=row, rollback_on_failure=True)
                    elif method == "updates":
                        result = LAYER_OBJECT.edit_features(updates=row, rollback_on_failure=True)
                    elif method == "deletes":
                        result = LAYER_OBJECT.edit_features(deletes=row, rollback_on_failure=True)
                except Exception as e:
                    if '504' in str(e):
                        # let's try ignoring timeouts for now, it seems the count catches up eventually
                        continue

            # If we didn't get rolled back, batch of adds successfully added.
            else:
                success = True

    # Wrapped AGO function in a retry while loop because AGO is very unreliable.
    def delete_features(wherequery):
        count = 0
        while True:
            if count > 5:
                raise RuntimeError("AGO keeps failing on our delete query!")
            try:
                LAYER_OBJECT.delete_features(where=wherequery)
                break
            except RuntimeError as e:
                if 'request has timed out' in str(e):
                    print(f'Request timed out, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                if 'Unable to perform query' in str(e):
                    print(f'Dumb error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                if '502' in str(e):
                    print(f'502 Gateway error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(15)
                    continue
                else:
                    raise e

    # Wrapped AGO function in a retry while loop because AGO is very unreliable.
    def query_features(wherequery=None, outstats=None):
        count = 0
        while True:
            if count > 5:
                raise RuntimeError("AGO keeps failing on our query!")
            try:
                # outstats is used for grabbing the MAX value of updated_datetime.
                if outstats:
                    output = LAYER_OBJECT.query(outStatistics=outstats, outFields='*')
                elif wherequery:
                    output = LAYER_OBJECT.query(where=wherequery)
                return output
            except RuntimeError as e:
                if 'request has timed out' in str(e):
                    print(f'Request timed out, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Ambiguous mysterious error returned to us sometimes1
                if 'Unable to perform query' in str(e):
                    print(f'Dumb error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                if '502' in str(e):
                    print(f'502 Gateway error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(15)
                    continue
                else:
                    raise e


    ##########################################
    # Steps
    # 1. Grab the max date in AGO
    # 2. Compare against max date in databridge and grab rows between that date and the latest
    # 3. Query record in AGO and delete row in if it exists
    # 4. Format record for AGO and append


    # First let's do a pre-check and assert column headers are what we expect.
    expected_headers = ['CLOSED_DATETIME','OBJECTID','SERVICE_REQUEST_ID','STATUS','STATUS_NOTES','SERVICE_NAME',
                        'SERVICE_CODE','DESCRIPTION','AGENCY_RESPONSIBLE','SERVICE_NOTICE','REQUESTED_DATETIME',
                        'UPDATED_DATETIME','EXPECTED_DATETIME','CLOSED_DATETIME','ADDRESS','ZIPCODE','MEDIA_URL','PRIVATE_CASE',
                        'DESCRIPTION_FULL','SUBJECT','TYPE_','SHAPE',]

    headers_stmt = f'''
    SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = 'SALESFORCE_CASES'
        AND OWNER = 'GIS_311'
    '''
    cursor.execute(headers_stmt)
    headers = cursor.fetchall()

    headers_list = []
    for i in headers:
        if i[0] not in expected_headers:
            raise AssertionError(f'Unexpected column!!: {i}')
        headers_list.append(i[0])

    # We don't want this in our SELECT statements, we'll instead grab them later like this:
    # sde.st_astext(SHAPE) as SHAPE
    # AGO also doesn't return shape as a field so we need this out for a field comparison.
    headers_list.remove('SHAPE')

    # Original list without our 'to_char' conversions so we can do an accurate field comparison to AGO
    # Copy the list so it's not just a memory reference
    headers_list_original = headers_list.copy()

    headers_list.append('sde.st_astext(SHAPE) as SHAPE')

    # NOTE: cx_Oracle has a bug where it doesn't return timezone information
    # so dates come in timezone naive.
    # https://github.com/oracle/python-cx_Oracle/issues/13
    # To get around this, convert datetime to string
    for i,header in enumerate(headers_list):
        if 'DATETIME' in header:
            headers_list[i] = "to_char({0},'YYYY-MM-DD HH24:MI:SS TZHTZM') AS {0}".format(header)

    # Join into string to be used in select statements.
    headers_str = ','.join(headers_list)

    # Compare headers/fields vs AGO, lowercase for proper comparison
    ago_fields = [i.name.lower() for i in LAYER_OBJECT.properties.fields]
    db_fields =  [x.lower() for x in headers_list_original]

    ago_fields.sort()
    db_fields.sort()

    print(f'Comparing AGO fields: "{ago_fields}" and databridge fields: "{db_fields}"')
    assert db_fields == ago_fields

    ###############################
    # 1. Grab the max date in AGO

    # check if start date was passed, we'll grab records from that point forward.
    if day:
        max_ago_dt = datetime.datetime.strptime(day, '%Y-%m-%d')
        max_ago_dt_str = max_ago_dt.strftime("%Y-%m-%d %H:%M:%S")
        print(f'\nStart date passed, grabbing records starting at {max_ago_dt_str}')
        end_dt = max_ago_dt + datetime.timedelta(days=1)
        end_dt_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    else:
        # Grab the max UPDATED_DATETIME from AGO.
        print('\nGrabbing max updated_datetime from AGO...')
        outstats = [{"statisticType":"max", "onStatisticField": "UPDATED_DATETIME"}]
        latest_time = query_features(outstats=outstats)

        # It gets returned as a unix timestamp, so convert it back.
        print('Unix time returned by AGO: ' + str(latest_time.sdf['MAX_UPDATED_DATETIME'][0]))
        assert latest_time.sdf['MAX_UPDATED_DATETIME'][0]

        # Datetime expects a "seconds" formatted unix timestamp, not in milliseconds. Convert by dividing by 1000
        max_ago_dt = datetime.datetime.fromtimestamp( latest_time.sdf['MAX_UPDATED_DATETIME'][0]/ 1000 )

        # AGO is doing something funny where it returns the timestamp as timezone naive UTC
        # so we get a UTC timestamp, but when we attempt to convert to local time, it again subtracts 4 or 5 hours
        # Which then puts us 8 hours behind...
        # We need this in local time to query Databridge effectively, so we'll manually adjust it.
        # So my solution is to simply check if the timestamp is in DST or not,
        # add the hours myself and then keep it timezone naive *shrug
        if max_ago_dt.dst() != datetime.timedelta(0):
            max_ago_dt = max_ago_dt + datetime.timedelta(hours=4)
        else:
            max_ago_dt = max_ago_dt + datetime.timedelta(hours=5)

        max_ago_dt_str = max_ago_dt.strftime("%Y-%m-%d %H:%M:%S")

        print('Max AGO Timestamp after timezone correction: ' + str(max_ago_dt_str) + '\n')

    ############################################
    # 2. Compare against max date in databridge
    # grab updated_datetime and order by it so we can iterate through our primary keys in proper temporal order.

    # If a day param was a passed, grab only records for that day.
    if day:
        databridge_stmt=f'''
        SELECT {PRIMARY_KEY},UPDATED_DATETIME
            FROM GIS_311.SALESFORCE_CASES
            WHERE UPDATED_DATETIME >= to_date('{max_ago_dt_str}', 'YYYY-MM-DD HH24:MI:SS')\
            AND UPDATED_DATETIME < to_date('{end_dt_str}', 'YYYY-MM-DD HH24:MI:SS')\
            ORDER BY UPDATED_DATETIME ASC
        '''
    # Else grab recrods from the max updated_datetime we have in AGO and forward
    else:
        databridge_stmt=f'''
        SELECT {PRIMARY_KEY},UPDATED_DATETIME
            FROM GIS_311.SALESFORCE_CASES
            WHERE UPDATED_DATETIME >= to_date('{max_ago_dt_str}', 'YYYY-MM-DD HH24:MI:SS')\
            ORDER BY UPDATED_DATETIME ASC
        '''

    print(f'Grabbing all {PRIMARY_KEY}s with same date or greater with query: {databridge_stmt}')
    cursor.execute(databridge_stmt)
    cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
    databridge_matches = cursor.fetchall()

    if len(databridge_matches) == 0:
        print('Nothing to update!')
        return
    print(f'\nTotal amount of rows to update in AGO from Databridge: {len(databridge_matches)}\n')
    first = databridge_matches[0]['SERVICE_REQUEST_ID']
    #print(f'First in databridge return, service_request_id: {first}')

    ##############################################
    # 3. Loop through returned databridge rows
    adds = []
    delsquery = '' 
    for row in databridge_matches:
        working_primary_key = row[PRIMARY_KEY]

        # query record in AGO and delete row in if it exists
        wherequery = f'{PRIMARY_KEY} = {working_primary_key}'

        # Wrap query in a while loop cause it times out sometimes
        ago_row = query_features(wherequery)

        # Grab the full row from databridge
        databridge_stmt = f'''
            SELECT {headers_str}
                FROM GIS_311.SALESFORCE_CASES
                WHERE {PRIMARY_KEY}  = {working_primary_key}
        '''
        cursor.execute(databridge_stmt)
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
        new_row = cursor.fetchall()
        if len(new_row) > 1:
            raise AssertionError(f'Should have only gotten 1 row back, instead got this many: {len(new_row)}')
        if len(new_row) == 0:
            # Retry once more against databridge if we encounter this
            # I don't know why oracle is doing this occasionally, the queries always work when I run them manually
            sleep(10)
            cursor.execute(databridge_stmt)
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            new_row = cursor.fetchall()
            if len(new_row) == 0:
                raise AssertionError(f'Got nothing back from Databridge with query: {databridge_stmt}')
            if len(new_row) > 1:
                raise AssertionError(f'Should have only gotten 1 row back, instead got this many: {len(new_row)}')
            print('INFO: Had to retry oracle query')

        # Access our row dictionary from the return of cursor.fetchall()
        new_row = new_row[0]


        # Lowercase all keys, as AGO expects lowercase field names.
        # Edaait: not sure if this is actually true.
        new_row = {k.lower(): v for k, v in new_row.items()}

        # apply various transformations, projections (if necessary) and fixes to our row.
        # Format it into proper format for uploading to AGO
        # Reference: https://developers.arcgis.com/python/guide/editing-features/
        row_to_append = format_row(new_row)

        # A true AGO upsert requries some more complex comparing between the rows we have
        # what's in AGO, and also matching up the objectid. We can avoid that by simply
        # deleting the row, which we'll then add again ourselves.
        if not ago_row.sdf.empty:
            delsquery = delsquery + f' {PRIMARY_KEY} = {working_primary_key} OR'

        #if ago_row.sdf.empty:
            #print(f'New row: {PRIMARY_KEY}: {working_primary_key}')

        adds.append(row_to_append)

        # Accumulate our adds and deletes like so until they reach these limits
        # Then apply in batch
        # A bit messy but it should (probably) save some strain on ESRI's infra and go faster
        # then one at a time.
        # AGO will also fail hard if our delsquery of multiple OR statements gets too long.
        if len(adds) >= 20 or len(delsquery) >= 350:
            print('\nApplying batch dels and adds to AGO..')
            if delsquery:
                # This is messy but slice it to remove trailing ' OR' otherwise the query is invalid
                #print(f'delsquery: {delsquery[:-3]}')
                delete_features(delsquery[:-3])
                print(f'Deleted {delsquery.count("=")} rows.')
            if adds:
                # Print the last primary_key of the last item in our adds
                # just so we have some sense of where we're at, if say we're staring at
                # logs and going insane.
                print('On {}: {}'.format(PRIMARY_KEY.lower(), adds[-1:][0]['attributes'][PRIMARY_KEY.lower()]))
                #print(f'adds: {adds}')
                edit_features(adds, method='adds')
                print(f'Added {len(adds)} rows.')
            adds = []
            delsquery = ''

    # Apply last leftover batch
    print('\nApplying last leftover batch dels and adds to AGO..')
    if delsquery:
        # This is messy but slice it to remove trailing ' OR' otherwise the query is invalid
        #print(f'delsqery: {delsquery[:-3]}')
        delete_features(delsquery[:-3])
        print(f'Deleted {delsquery.count("=")} rows.')
    if adds:
        print('On {}: {}'.format(PRIMARY_KEY.lower(), adds[-1:][0]['attributes'][PRIMARY_KEY.lower()]))
        #print(f'adds: {adds}')
        edit_features(adds, method='adds')
        print(f'Added {len(adds)} rows.')
    print('Done.')

if __name__ == '__main__':
    sync()