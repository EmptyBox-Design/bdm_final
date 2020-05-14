# converts violation county code abriveation to 
# centerline code  1 - 5
def getCounty(county):
    county_dict =[
        ['MAN', 'MH', 'MN', 'NEWY', 'NEW', 'Y', 'NY'], 
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

                key = "_".join(violation_row)

                counts[key] = counts.get(key, 0) +1

    return counts.items()

def compareTupes(low, high, test):

    try:
        a = low.split("-")
        a = (int(a[0]), int(a[1]))

        b = high.split("-")
        b = (int(b[0]), int(b[1]))

        t = test.split("-")
        t = (int(t[0]), int(t[1]))

        if(t >= a and t <= b):
            return True
        else:
            return False
    except IndexError:
        return False
    except AttributeError:
        return False

def matchHouseNumber(hn, odd_house, even_house):
    
    match = False
    
    try:
        # house number is an integer
        violation_house_number = int(hn)

        if((violation_house_number % 2) == 0):

            # house number is even
            c_low = even_house[0]
            c_high = even_house[1]

            try:
                # checks if violation house number is greater or equal to min
                # checks if violation house number is less than or equal to max
                if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
                    # print('match')
                    # this means that the street name of full street name
                    # matched and house number is in range
                    match = True
                    
            except ValueError:
                # centerline data is a compound address
                # returns True or False based on match
                if(compareTupes(c_low, c_high, violation_house_number)):
                    match = True

        else:
            # house number is odd
            c_low = odd_house[0]
            c_high = odd_house[1]

            try:
                if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
                    # this means that the street name of full street name
                    # matched and house number is in range
                    match = True
            except ValueError:
                # centerline data is a compound address
                # returns True or False based on match
                if(compareTupes(c_low, c_high, violation_house_number)):
                    match = True
    # the violation house number is a tuple or at least not a numerical integer               
    except ValueError:
        # the violation house number is not a integer
        # need to split it and treat it as a tuple
        # checking the second value for even or odd
        try:
            if((int(hn.split("-")[1]) % 2) == 0):
                # violation house number is even
                c_low = even_house[0]
                c_high = even_house[1]

                if(compareTupes(c_low, c_high, hn)):
                    match = True
            else:
                c_low = odd_house[0]
                c_high = odd_house[1]

                if(compareTupes(c_low, c_high, hn)):
                    match = True
        # catching errors if value is not an integer such as if it is a compound value with alphabetical values
        # or it is compound and is not a street we can match in a range
        except IndexError:
            match = False
        except ValueError:
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

# returns centerline table as dictionary
# by street_label = data
# by full_street = data
def createLookupTable(data):
    
    table = {}
    
    for d in data:
        table[d[0]] = d[1]

    return table

# writes data csv 
# unpacks value tuples
def toCSVLine(data):
    return ','.join(str(e) for e in data)

# violation example joined by
# [house_number, street_name, county, year]
# currently returns NONE if no match is made
# which means that the given violation did not match a centerline
def mapToCenterLineData(record, cscl_data):

    d = record[0].split("_")
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
        if(matchHouseNumber(d[0], cscl_data[key][1], cscl_data[key][2])):
            return (new_key, record[1])

if __name__ == "__main__":

    from pyspark import SparkContext
    sc = SparkContext()

    import sys

    output_location = sys.argv[1]

    violation_data_file_location = "hdfs:///tmp/bdm/nyc_parking_violation/"
    # violation_data_file_location = "./Data/2018.csv"
    cscl_data_location = "hdfs:///tmp/bdm/nyc_cscl.csv"
    # cscl_data_location = "./Data/nyc_cscl.csv"
    
    cscl_read = sc.textFile(cscl_data_location)
    
    cscl_data_map = cscl_read.mapPartitionsWithIndex(readCenterLineDataRDD)
    
    cscl_data = createLookupTable(cscl_data_map.collect())

    cscl_data_broadcast = sc.broadcast(cscl_data).value

    rdd = sc.textFile(violation_data_file_location)
    
    counts = rdd.mapPartitionsWithIndex(processViolations) \
        .map(lambda data: mapToCenterLineData(data, cscl_data_broadcast)) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda x,y: x+y) \
        .map(toCSVLine) \
        .saveAsTextFile(output_location)

    print('done processing!')