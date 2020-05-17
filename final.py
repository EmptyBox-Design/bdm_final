# inputs a violation number
# returns integer value or tuple of integers
# strips out letters
# returns None for no matches for only letters and --
def getHouseNumber(hn):
    
    match = None
    
    import re

    # remove all alphabetical letters from string
    def stripAZ(string):
        return re.sub('\D', '', string.strip())

    # does the house number contain any digits
    if(re.search(r'\d', hn)):
        # try to process it as an integer
        try:
            hn = int(hn)
            match = ("int",hn)
        # if fail try to process as compound split by "-"
        except ValueError:

            # split the house number
            test_split = hn.split("-")
            
            # if the house number split only contains one value
            # treat it as an integer and strip 
            if(len(test_split) == 1):
                hn = stripAZ(hn)
                match = ("int",int(hn))
            # if it is compound
            # strip out letters
            # test and return 
            elif(len(test_split) == 2):
                
                # remove all letters from the tuple
                strip = (stripAZ(test_split[0]), stripAZ(test_split[1]))

                if(len(strip[0]) > 0 and len(strip[1]) > 0):
                    match = ('compound', (strip[0], strip[1]))
                else:
                    # this tests whethere the tuple has only one number on either side of the -
                    # return only that number
                    if(len(strip[0]) > 0):
                        match = ("int",int(strip[0]))
                    elif(len(strip[1]) > 0):
                        match = ("int",int(strip[1]))
            elif(match is None):
                # split the house number
                test_split = hn.split("--")

                # if the house number split only contains one value
                # treat it as an integer and strip 
                if(len(test_split) == 1):
                    hn = stripAZ(hn)
                    match = ("int",int(hn))
                # if it is compound
                # strip out letters
                # test and return 
                elif(len(test_split) == 2):

                    # remove all letters from the tuple
                    strip = (stripAZ(test_split[0]), stripAZ(test_split[1]))

                    if(len(strip[0]) > 0 and len(strip[1]) > 0):
                        match = ('compound', (strip[0], strip[1]))
                    else:
                        # this tests whethere the tuple has only one number on either side of the -
                        # return only that number
                        if(len(strip[0]) > 0):
                            match = ("int",int(strip[0]))
                        elif(len(strip[1]) > 0):
                            match = ("int",int(strip[1]))

        return match
    else:
        return None

# converts violation county code abriveation to 
# centerline code  1 - 5
def getCounty(county):
    county_dict =[
        ['MAN', 'MH', 'MN', 'NEWY', 'NEW', 'Y', 'NY', 'NEW Y'], 
        ['BRONX', 'BX', 'PBX'], 
        ['BK', 'K', 'KING', 'KINGS'],
        ['Q', 'QN', 'QNS', 'QU', 'QUEEN'],
        ['R', 'RICHMOND', 'ST']]

    match = None
    for row in enumerate(county_dict):
        if(county.upper() in row[1]):
            match = row[0] + 1

    return str(match)

def getYear(year):

    match = None

    try:
        match = year.split("/")[2]
    except IndexError:
        match = None
    return match

# return cleaned violations in tuple with the key being 
def processViolations(pid, records):
    
    import csv 

    if(pid == 0):
        next(records)
    
    reader = csv.reader(records)

    for row in reader:

        test_row = [row[4], row[21], row[23], row[24]]

        if(None not in test_row):
            # checks if values are empty string or string with no data besides whitespace
            if(all(len(i.strip()) > 0 for i in test_row)):
                
                year = getYear(row[4])

                if(year is not None):
                    if(int(year) >= 2015):

                        county = getCounty(row[21])

                        house_number = row[23]

                        street_name = row[24].upper()

                        violation_row = [house_number, street_name, county, year]

                        key = "__".join(violation_row)

                        yield (key, 1)


def matchHouseNumber(hn, odd_house, even_house):
    
    match = False
    
    checkHouseNumber = getHouseNumber(str(hn))
    
    def compareHouseNumberAsInt(hn, c_low, c_high):
        try:
            if(hn >= int(c_low) and hn <= int(c_high)):
                return True
            else:
                return False
        except ValueError:
            return False

    # compares a given violation compound house number
    # with compound centerline datapoints
    # returns true or false if a match is made
    def compareTupes(test,low,high):
        try:

            a = low.split("-")
            a = int(str(a[0]) + str(a[1]))

            b = high.split("-")
            b = int(str(b[0]) + str(b[1]))

            z = int(str(test[0]) + str(test[1]))

            if(z >= a and z <= b):
                return True
            else:
                return False
        except IndexError:
            return False
        except AttributeError:
            return False
        except ValueError:
            return False
        
    if(checkHouseNumber is not None):
        # violation house number is an integer
        # assumes that a integer house number can only match with a integer centerline value
        house_type = checkHouseNumber[0]
        hn = checkHouseNumber[1]
        
        if(house_type == "int"):
            
            if((hn % 2) == 0):
                match = compareHouseNumberAsInt(hn, even_house[0], even_house[1])
            else:
                match = compareHouseNumberAsInt(hn, odd_house[0], odd_house[1])
                
        # violation house number is compound
        elif(house_type == 'compound'):
            if((int(hn[1]) % 2) == 0):
                match = compareTupes(hn, even_house[0], even_house[1])
            else:
                match = compareTupes(hn, odd_house[0], odd_house[1])
    else:
        match = False
    # returns either True or False
    return match

# Read the centerline data
# parse the data to get only the fields we need
# return the parsed dataset as a two dimensional array
def readCenterLineDataRDD(pid, records):

    import csv

    if(pid == 0):
        next(records)

    reader = csv.reader(records)

    for row in reader:

        boro = row[13]

        physicalID = row[0]

        odd_house = [row[2], row[3]]

        even_house = [row[4], row[5]]
        
        full_street_key = (row[28].upper(), boro)

        street_label_key = (row[10].upper(), boro)

        yield((full_street_key, street_label_key), (physicalID, odd_house, even_house))


# writes data csv 
# unpacks value tuples
def toCSVLine(data):
    string = []
    
    for d in data:
        if(type(d) is list):
            string.append(','.join(str(e) for e in d))
        else:
            string.append(d)
    return ','.join(str(e) for e in string )

# def toCSVLine(data):
#   return ','.join(str(d) for d in data)
# violation example joined by
# [house_number, street_name, county, year]
# currently returns NONE if no match is made
# which means that the given violation did not match a centerline
def mapToCenterLineData(record, cscl_data):
    
    import re
    
    d = record[0].split("__")
    # key is violation street_name and county 
    key = (d[1], d[2])

    match = None
    
    # return((key), 0)
    # checks to see if violation street name matches fullstreet or st label in centerline data by key
    if key in cscl_data:

        # street matches need to check if any of the house numbers match
        # 0 - physcicalID
        # low
        # high
        for house_range in cscl_data[key]:

            # takes violation house number and odd_house and even_house as inputs
            # returns true or false if a match is made
            # violation house number, odd_house, even_house
            if(matchHouseNumber(d[0], house_range[1], house_range[2])):

                physicalID = house_range[0]

                year = d[3]

                new_key = physicalID + "-" + year

                match = (new_key, int(record[1]))
    else:
        match = None

    return match

# input value as a nested tuple
# returns list of flattened tuples
def unpackTupes(data):

    import statsmodels.api as sm

    years = {
        "2015":0,
        "2016" :0,
        "2017": 0,
        "2018": 0,
        "2019": 0
    }

    def foo(a, b=None):
        if a in years:
            years[a] = b

    if(data[1] is not None):
        for i in data[1]:
            foo(*i)

    def convertToInts(test_list):
        return [int(i) for i in test_list] 

    Y = convertToInts(list(years.values()))

    X = convertToInts(list(years.keys()))

    X = sm.add_constant(X)

    model = sm.OLS(Y,X)

    results = model.fit()

    year_ols = results.params[1]

    j = list(years.values())

    j.append(year_ols)

    return j

def keyGen(pid, records):
    
    import csv

    if(pid == 0):
        next(records)

    reader = csv.reader(records)

    for row in reader:

        physicalID = row[0]

        yield(physicalID, 0)

if __name__ == "__main__":

    import time
    start_time = time.time()

    from pyspark import SparkContext
    sc = SparkContext()

    import sys

    output_location = sys.argv[1]

    violation_data_file_location = "hdfs:///tmp/bdm/nyc_parking_violation/*.csv"
    # violation_data_file_location = "./Data/data/*.csv"
    cscl_data_location = "hdfs:///tmp/bdm/nyc_cscl.csv"
    # cscl_data_location = "./Data/nyc_cscl.csv"

    cscl_data_read = sc.textFile(cscl_data_location)

    cscl_data_map = cscl_data_read.mapPartitionsWithIndex(readCenterLineDataRDD) \
        .flatMap(lambda x: ( ((x[0][0]), (x[1])),   ((x[0][1]), (x[1]))  )) \
        .groupByKey() \
        .collectAsMap()

    print("created cscl dictionary",len(cscl_data_map.keys()))
    
    cscl_data_broadcast = sc.broadcast(cscl_data_map).value

    cscl_keys = cscl_data_read.mapPartitionsWithIndex(keyGen) \
        .reduceByKey(lambda x,y: x+y) \
        .sortByKey()

    rdd = sc.textFile(violation_data_file_location)
    
    counts = rdd.mapPartitionsWithIndex(processViolations) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda data: mapToCenterLineData(data, cscl_data_broadcast)) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x[0].split("-")[0], (x[0].split("-")[1], x[1]))) \
        .groupByKey() \
        .map(lambda x: (x[0], sorted(x[1], key=lambda z: z[0], reverse=False))) \
        
    joined = cscl_keys.leftOuterJoin(counts)

    joined.mapValues(lambda x: unpackTupes(x)) \
        .map(toCSVLine) \
        .saveAsTextFile(output_location)

        # .mapValues(lambda x: unpackTupes(x)) \
        # .map(toCSVLine) \
        # .saveAsTextFile(output_location)


    print ("done processing!", ((time.time() - start_time) / 60), " minutes to run")