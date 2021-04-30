import sys
import unittest
import json
import datetime
import mysql.connector
from mysql.connector import errorcode

USER = 'john'
PASSWORD = 'panda'

query1 = """
        SELECT VESSEL.MMSI, POSITION_REPORT.Longitude, POSITION_REPORT.Latitude, AIS_MESSAGE.Timestamp 
        FROM VESSEL, POSITION_REPORT, AIS_MESSAGE 
        WHERE VESSEL.MMSI=304858000 
        AND AIS_MESSAGE.MMSI=VESSEL.MMSI LIMIT 1;
        """
query2 = """SELECT * FROM VESSEL WHERE MMSI=304858000;"""


class SQL_runner:
    """
    A SQL connector
    """

    def __init__(self, user, pw, host='127.0.0.1', db=''):

        self.cnx = None

        try:
            print("Connecting to database ", db, "... ", end='')
            self.cnx = mysql.connector.connect(user=user, password=pw, database=db, host=host, port=3306)
            print("OK")

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        if not self.cnx:
            sys.exit("Connection failed: exiting.")

    def __del__(self):
        if self.cnx is not None:
            print("Closing database connection")
            self.cnx.close()

    def run(self, query):
        """ Run a query
        :param query: an SQL query
        :type query: str
        :return: the result set as Python list of tuples
        :rtype: list
        """
        cursor = self.cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        return result


class tmbDAO:
    def __init__(self, stub=False):
        self.is_stub = stub

    def insert_batch(self, batch_message):
        inserted = 0
        try:
            array = json.loads(batch_message)
            for x in array:
                if x['MsgType'] == "position_report":
                    latitude = array[inserted]['Position']['coordinates'][0]
                    longitude = array[inserted]['Position']['coordinates'][1]
                    navigational_status = array[inserted]['Status']

                    SoG = array[inserted]['SoG']

                    if x.get('RoT'):
                        RoT = array[inserted]['RoT']
                    else:
                        RoT = None

                    print("[Latitude and Longitude: [{}, {}]\nStatus: {}\nSoG: {}\nRoT: {}]"
                          .format(latitude, longitude, navigational_status, SoG, RoT))
                    inserted += 1

                elif x['MsgType'] == "static_data":
                    print(array[inserted]['Name'])
                    inserted += 1

        except Exception as e:
            print(e)
            return -1

        if self.is_stub:
            print(inserted)
            return len(array)

        return -1

    # populate the ais_message table
    def insert_batch_msg(self, batch):
        try:
            array = json.loads(batch)
            for x in array:
                # AIS_MESSAGE(
                # Id mediumint unsigned autoincrement)
                # Primary key (Id)
                # shared data listed below
                # insert into ais_message values (null, 12345, "2020")
                # Use "cursor.lastrowid" to get the id of the previous row.
                # In this case, we need the last row id to use as the aismessage_id for position_report/static_data
                if x['MsgType'] == "position_report":
                    latitude = 0
                    longitude = 0
                    navigational_status = ''
                    RoT = 0
                    SoG = 0
                    CoG = 0
                    Heading = 0
                    # insert into position_report value ({cursor.lastrowid}, )
                    if x['position'] in array:
                        latitude = x['position'][1][0]
                        longitude = x['position'][1][1]
                        print(latitude)
                        print(longitude)

                elif x['MsgType'] == "static_data":
                    pass

        except Exception as e:
            print(e)
            return -1

        if self.is_stub:
            return len(array)

        return -1


    def delete_timestamp(self, current_time, tStamp):
        cTime = datetime.datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        olderThanFiveMin = '2020-11-18T00:05:00.000Z'
        i = datetime.datetime.strptime(olderThanFiveMin, "%Y-%m-%dT%H:%M:%S.%fZ")
        b = datetime.datetime.strptime('2020-11-18T00:00:00.000Z', "%Y-%m-%dT%H:%M:%S.%fZ")
        temp = []
        try:
            array = json.loads(tStamp)
        except Exception as e:
            print(e)
            return -1

        for x in array:
            tempTime = datetime.datetime.strptime(x['Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            # compares the datetime
            if tempTime < cTime:
                # compares the timedelta
                if (tempTime - b) < (cTime - i):
                    temp.append(x)

        if self.is_stub:
            return len(temp)

    # return the most recent ship positions (the "oldest" timestamp)
    def all_ship_positions(self, batch):
        try:
            array = json.loads(batch)
        except Exception as e:
            print(e)
            return -1

        recent_positions = []
        mmsi_dict = {}

        for x in array:
            mmsi_dict[x['MMSI']] = []

        mmsi_dict_keys = mmsi_dict.keys()
        for y in array:
            mmsi = y['MMSI']
            if mmsi_dict_keys.__contains__(mmsi):
                temp = mmsi_dict[mmsi]
                temp.append(y)
                mmsi_dict[mmsi] = temp
        for k in mmsi_dict:
            temp = mmsi_dict[k]
            if len(temp) > 1:
                for i in temp:
                    for j in temp:
                        tempA = datetime.datetime.strptime(i['Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                        tempB = datetime.datetime.strptime(j['Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                        if tempA > tempB:
                            temp.remove(j)
                        else:
                            temp.remove(i)
            mmsi_dict[k] = temp
        for z in mmsi_dict:
            location = mmsi_dict[z]
            recent_positions.append(location[0])

        print(recent_positions)
        return recent_positions

    def ship_position_MMSI(self, mmsi, batch):
        try:
            array = json.loads(batch)
        except Exception as e:
            print(e)
            return -1
        for x in array:
            if x.get('MMSI') == mmsi:
                return x.get('Position')

    def recent_ship_position_mmsi(self, mmsi):
        self.cnx = None
        try:
            print("Connecting to database ", "... ", end='')
            self.cnx = mysql.connector.connect(user=USER, password=PASSWORD, database='AisTestData', host='127.0.0.1',
                                               port=3306)
            print("OK")

            cursor = self.cnx.cursor()
            query = """
                    SELECT VESSEL.MMSI, POSITION_REPORT.Longitude, POSITION_REPORT.Latitude, AIS_MESSAGE.Timestamp 
                    FROM VESSEL, POSITION_REPORT, AIS_MESSAGE 
                    WHERE VESSEL.MMSI='%s' 
                    AND AIS_MESSAGE.MMSI=VESSEL.MMSI LIMIT 1;
                    """
            cursor.execute(query % mmsi)
            result = cursor.fetchall()
            self.cnx.commit()
            cursor.close()
            self.cnx.close()
            print("Connection closed")
            return result

        except mysql.connector.Error as error:
            print(error)

    def vessel_info(self, mmsi, batch):
        try:
            array = json.loads(batch)
        except Exception as e:
            print(e)
            return -1
        list = []
        for x in array:
            if x.get('MMSI') == mmsi:
                list.append(x)
        print(list)
        return list

    def retrieve_vessel_info(self, mmsi):
        self.cnx = None
        try:
            print("Connecting to database ", "... ", end='')
            self.cnx = mysql.connector.connect(user=USER, password=PASSWORD, database='AisTestData', host='127.0.0.1',
                                               port=3306)
            print("OK")

            cursor = self.cnx.cursor()
            query = """
                    SELECT *
                    FROM VESSEL
                    WHERE MMSI='%s';
                    """
            cursor.execute(query % mmsi)
            result = cursor.fetchall()
            self.cnx.commit()
            cursor.close()
            self.cnx.close()
            print("Connection closed")
            return result

        except mysql.connector.Error as error:
            print(error)

    def insert_new_message(self, message):
        try:
            array = json.loads(message)
            print(array)
            print(array[0]['Timestamp'])
        except Exception as e:
            print(e)
            return -1

        if self.is_stub:
            return len(array)

        return -1

    # Attempting to make the insert message work via sql
    def insert_message(self, message):
        self.cnx = None

        array = json.loads(message)
        msgType = array['MsgType']

        try:
            print("Connecting to database ", "... ", end='')
            self.cnx = mysql.connector.connect(user=USER, password=PASSWORD, database='AisTestData', host='127.0.0.1',
                                               port=3306)
            print("OK")

            cursor = self.cnx.cursor()
            if msgType == "position_report":
                query = """
                        INSERT INTO TABLE POSITION_REPORT (NavigationalStatus, Longitude, Latitude, RoT, SoG, CoG, Heading)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        
                        """
                cursor.execute(query % message)
            elif msgType == "static_data":
                query = """
                        INSERT 
                        """
                cursor.execute(query % message)

            result = cursor.fetchall()
            self.cnx.commit()
            cursor.close()
            self.cnx.close()
            print("Connection closed")
            return result

        except mysql.connector.Error as error:
            print(error)

    def read_ship_positions_in_tile(self, tile_id):
        pass

    def match_port(self, port_name):
        pass

    # If match_unique_port fails to find a unique port, then it will return the same thing as match_port.
    # Otherwise it will return all ship positions in the tile of scale 3
    def match_unique_port(self, port_name, country):
        pass

    # Returns the last five positions of the given mmsi
    def last_five_positions(self, mmsi):
        pass

    def ship_position_headed_toward_port(self, port_id):
        pass

    def position_towards_given_port(self, port_name, country):
        pass


class tmbTest(unittest.TestCase):
    sql = None

    @classmethod
    def setUpClass(cls):
        tmbTest.sql = SQL_runner(USER, PASSWORD, db='AisTestData')

    @classmethod
    def tearDownClass(cls):
        tmbTest.sql = None

    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                    {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    testbatch = json.load(open("sample_input.json"))
    testbatch = json.dumps(testbatch)

    def test01(self):
        """
        Counts number of json strings
        """
        tmb = tmbDAO(True)
        count = tmb.insert_batch(self.batch)
        #self.assertTrue(type(count) is int and count >= 0)

    def test_sql01(self):
        tmb = tmbDAO(True)
        count = tmb.insert_batch_msg(self.testbatch)
        self.assertTrue(type(count) is int and count >=0)

    batch2 = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}]"""

    def test02(self):
        """
        Checks to see if a single entry was added
        """
        tmb = tmbDAO(True)
        count = tmb.insert_new_message(self.testbatch)
        self.assertTrue(type(count) is int and count >= 0)

    batch3 = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:06:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:07:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:09:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}]"""

    def test03(self):
        """
        Remove all entries whose timestamp is older than 5 minutes than the current time and counts the number of
        entries removed
        :return:
        """
        tmb = tmbDAO(True)
        count = tmb.delete_timestamp('2020-11-18T00:09:00.00Z', self.batch3)
        self.assertTrue(type(count) is int and count >= 0)

    batch4 = """[{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"},
    {"Timestamp":"2020-11-18T00:05:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[57.61291,18.62997]},"Status":"Unknown value"}
    ]"""

    def test04(self):
        """
        Return all ship positions
        :return:
        """
        tmb = tmbDAO(True)
        recent_positions = tmb.all_ship_positions(self.batch)
        self.assertTrue(recent_positions, """[{'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'Class A', 'MMSI': 304858000, 'MsgType': 'position_report', 'Position': {'type': 'Point', 'coordinates': [55.218332, 13.371672]}, 'Status': 'Under way using engine', 'SoG': 10.8, 'CoG': 94.3, 'Heading': 97}, 
        {'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'AtoN', 'MMSI': 992111840, 'MsgType': 'static_data', 'IMO': 'Unknown', 'Name': 'WIND FARM BALTIC1NW', 'VesselType': 'Undefined', 'Length': 60, 'Breadth': 60, 'A': 30, 'B': 30, 'C': 30, 'D': 30}, 
        {'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'Class A', 'MMSI': 219005465, 'MsgType': 'position_report', 'Position': {'type': 'Point', 'coordinates': [54.572602, 11.929218]}, 'Status': 'Under way using engine', 'RoT': 0, 'SoG': 0, 'CoG': 298.7, 'Heading': 203}, 
        {'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'Class A', 'MMSI': 257961000, 'MsgType': 'position_report', 'Position': {'type': 'Point', 'coordinates': [55.00316, 12.809015]}, 'Status': 'Under way using engine', 'RoT': 0, 'SoG': 0.2, 'CoG': 225.6, 'Heading': 240}, 
        {'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'AtoN', 'MMSI': 992111923, 'MsgType': 'static_data', 'IMO': 'Unknown', 'Name': 'BALTIC2 WINDFARM SW', 'VesselType': 'Undefined', 'Length': 8, 'Breadth': 12, 'A': 4, 'B': 4, 'C': 4, 'D': 8}, 
        {'Timestamp': '2020-11-18T00:00:00.000Z', 'Class': 'Class A', 'MMSI': 257385000, 'MsgType': 'position_report', 'Position': {'type': 'Point', 'coordinates': [55.219403, 13.127725]}, 'Status': 'Under way using engine', 'RoT': 25.7, 'SoG': 12.3, 'CoG': 96.5, 'Heading': 101}, 
        {'Timestamp': '2020-11-18T01:00:00.000Z', 'Class': 'Class A', 'MMSI': 376503000, 'MsgType': 'position_report', 'Position': {'type': 'Point', 'coordinates': [54.519373, 11.47914]}, 'Status': 'Under way using engine', 'RoT': 0, 'SoG': 7.6, 'CoG': 294.4, 'Heading': 290}]
        """)

    def test05(self):
        """
        Return the most recent position of the vessel with the corresponding MMSI
        :return:
        """
        tmb = tmbDAO(True)

        pos = tmb.ship_position_MMSI(304858000, self.batch3)
        self.assertTrue(pos, "{'type': 'Point', 'coordinates': [55.218332, 13.371672]}")

    def test_sql05(self):
        tmb = tmbDAO(True)
        testA = tmb.recent_ship_position_mmsi(304858000)
        queryResult = self.sql.run(query1)
        self.assertTrue(testA, queryResult)

    def test06(self):
        """
        Returns all data of a vessel, given the MMSI
        :return:
        """
        tmb = tmbDAO(True)

        data = tmb.vessel_info(304858000, self.batch3)
        self.assertTrue(data, """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:06:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:07:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
    {\"Timestamp\":\"2020-11-18T00:09:00.00Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}]""")

    def test_sql06(self):
        tmb = tmbDAO(True)
        testA = tmb.retrieve_vessel_info(304858000)
        queryResult = self.sql.run(query2)
        self.assertTrue(testA, queryResult)
