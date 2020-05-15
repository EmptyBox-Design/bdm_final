# inputs a violation number
# returns integer value or tuple of integers
# strips out letters
# returns None for no matches for only letters and --
def getHouseNumber(hn):
    
    import re
    
    def stripAZ(string):
        return int(re.sub('\D', '', string))

    if(re.search(r'\d', hn)):
        try:
            hn = int(hn)
            match = ("int",hn)
        except ValueError:
            match = None
            test_split = hn.split("-")

            if(len(test_split) == 1):
                hn = stripAZ(hn)
                match = ("int",hn)

            elif(len(test_split) == 2):
                match = ('compound',(stripAZ(test_split[0]), stripAZ(test_split[1])))
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
            match = row[0]

    return str(match)

def getYear(year):
    match = None
    try:
        match = year.split("/")[2]
    except IndexError:
        match = None
    return str(match)

# return cleaned violations in tuple with the key being 
def processViolations(pid, records):
    
    counts = {}

    if(pid == 0):
        next(records)
        
    for record in records:
        
        row = record.split(',')
        
        test_row = [row[4], row[21], row[23], row[24]]
        #  checks if values are None
        if(None not in test_row):
            # checks if values are empty string or string with no data besides whitespace
            if(all(len(i.strip()) > 0 for i in test_row)):
                
                year = getYear(row[4])

                county = getCounty(row[21])

                house_number = row[23]

                street_name = row[24].lower()

                violation_row = [house_number, street_name, county, year]

                key = "__".join(violation_row)
                # adds 1 to the key if the same violation on same street and house number exists
                counts[key] = counts.get(key, 0) +1

    return counts.items()

def matchHouseNumber(hn, odd_house, even_house):
    
    match = False
    
    checkHouseNumber = getHouseNumber(hn)
    
    def compareHouseNumberAsInt(hn, c_low, c_high):
        try:
            if(int(hn) >= int(c_low) and int(hn) <= int(c_high)):
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
            a = (int(a[0]), int(a[1]))

            b = high.split("-")
            b = (int(b[0]), int(b[1]))

            if(test >= a and test <= b):
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
            if((hn[1] %2) == 0):
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
    
    # Skip the header
    if pid==0:
        next(records)

    # reader
    reader = csv.reader(records)
    
    # container
    data = {}

    for row in reader:

        boro = row[13]

        full_street_key = (row[28].lower(), boro)

        street_label_key = (row[10].lower(), boro)

        physicalID = row[0]

        odd_house = [row[2], row[3]]

        even_house = [row[4], row[5]]
        
        data[full_street_key]  = [physicalID, odd_house, even_house]
        data[street_label_key] = [physicalID, odd_house, even_house]

    return data.items()

# writes data csv 
# unpacks value tuples
def toCSVLine(data):
    return ','.join(str(e) for e in data)

# violation example joined by
# [house_number, street_name, county, year]
# currently returns NONE if no match is made
# which means that the given violation did not match a centerline
def mapToCenterLineData(record, cscl_data):

    import re

    d = record[0].split("__")
    # key is violation street_name and county 
    key = (d[1], d[2])

    # return((key), 0)
    # checks to see if violation street name matches fullstreet or st label in centerline data by key
    if (key) in cscl_data:

        physicalID = cscl_data[key][0]

        year = d[3]

        new_key = physicalID + "-" + year
        
        # takes violation house number and odd_house and even_house as inputs
        # returns true or false if a match is made
        if(re.search(r'\d', d[0])):
            if(matchHouseNumber(d[0], cscl_data[key][1], cscl_data[key][2])):
                return (new_key, int(record[1]))

# input value as a nested tuple
# returns list of flattened tuples
def unpackTupes(data):
    j = []
    
    def foo(a, b=None):
        j.append(a)
        j.append(b)

    for i in data:
        foo(*i)

    return j

if __name__ == "__main__":

    from pyspark import SparkContext
    sc = SparkContext()

    import sys

    output_location = sys.argv[1]

    violation_data_file_location = "hdfs:///tmp/bdm/nyc_parking_violation/"
    # violation_data_file_location = "./Data/2016.csv"
    cscl_data_location = "hdfs:///tmp/bdm/nyc_cscl.csv"
    # cscl_data_location = "./Data/nyc_cscl.csv"
    
    cscl_read = sc.textFile(cscl_data_location)
    
    cscl_data = cscl_read.mapPartitionsWithIndex(readCenterLineDataRDD).reduceByKey(lambda x,y: x+y).collectAsMap()

    cscl_data_broadcast = sc.broadcast(cscl_data).value

    rdd = sc.textFile(violation_data_file_location)
    
    counts = rdd.mapPartitionsWithIndex(processViolations) \
        .map(lambda data: mapToCenterLineData(data, cscl_data_broadcast)) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x[0].split("-")[0], (x[0].split("-")[1], x[1]))) \
        .groupByKey() \
        .map(lambda x: (x[0], sorted(x[1], key=lambda z: z[0], reverse=False))) \
        .mapValues(lambda x: unpackTupes(x)) \
        .map(toCSVLine) \
        .saveAsTextFile(output_location)

    print('done processing!')