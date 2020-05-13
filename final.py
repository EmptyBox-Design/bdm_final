# Read the centerline data
# parse the data to get only the fields we need
# return the parsed dataset as a two dimensional array
def readCenterLineData():
    
    # importing csv module 
    import csv 

    # csv file name 
    filename = "hdfs:///tmp/bdm/nyc_cscl.csv"

    # initializing the titles and rows list 
    fields = [] 
    cscl = {}

    # reading csv file 
    with open(filename, 'r') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 

        # extracting field names through first row 
        fields = next(csvreader) 

        # extracting each data row one by one
        # do not drop any rows
        # create a dictionary of fullstreet and duplicate for street label
        # house number information inside the object
        for row in csvreader:
            
            boro = row[13]
            
            full_street_key = (row[28], boro)
            
            street_label_key = (row[10], boro)

            cscl[full_street_key] = {
                "physicalID":  row[0],
                "odd_house": [row[2], row[3]],
                "even_house": [row[4], row[5]]
            }
            
            cscl[street_label_key] = {
                "physicalID":  row[0],
                "odd_house": [row[2], row[3]],
                "even_house": [row[4], row[5]]
            }

    return cscl

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

                street_name = row[24]

                violation_row = [house_number, street_name, county, year]

                key = "_".join(violation_row)

                counts[key] = counts.get(key, 0) +1

    return counts.items()

# def matchHouseNumber(violation_house_number):
    
#     try:
#         # house number is an integer
#         violation_house_number = int(hn)

#         if((violation_house_number % 2) == 0):

#             # house number is even
#             c_low = centerLineRow[3]
#             c_high = centerLineRow[4]

#             try:
#                 # checks if violation house number is greater or equal to min
#                 # checks if violation house number is less than or equal to max
#                 if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
#                     # print('match')
#                     # this means that the street name of full street name
#                     # matched and house number is in range
#                     match = ID
#             except ValueError:
#                 # centerline data is a compound address
#                 # returns True or False based on match
#                 if(compareTupes(c_low, c_high, violation_house_number)):
#                     match = ID

#         else:
#             # house number is odd
#             c_low = centerLineRow[0]
#             c_high = centerLineRow[1]

#             try:
#                 if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
#                     # this means that the street name of full street name
#                     # matched and house number is in range
#                     match = ID
#             except ValueError:
#                 # centerline data is a compound address
#                 # returns True or False based on match
#                 if(compareTupes(c_low, c_high, violation_house_number)):
#                     match = ID
#     # the violation house number is a tuple or at least not a numerical integer               
#     except ValueError:
#         # the violation hosue number is not a integer
#         # need to split it and treat it as a tuple
#         # checking the second value for even or odd
#         try:
#             if((int(hn.split("-")[1]) % 2) == 0):
#                 # violation house number is even
#                 c_low = centerLineRow[3]
#                 c_high = centerLineRow[4]

#                 if(compareTupes(c_low, c_high, hn)):
#                     match = centerLineRow[2]
#             else:
#                 c_low = centerLineRow[0]
#                 c_high = centerLineRow[1]

#                 if(compareTupes(c_low, c_high, hn)):
#                     match = centerLineRow[2]

#         except IndexError:
#             match = None
#         except ValueError:
#             match = None

def mapToCenterLineData(record, cscl_data):

    d = record[0].split("_")
    key = (d[1], d[2])

    # checks to see if violation street name matches fullstreet or st label in centerline data by key
    # if match check house number
    if key in cscl_data:
        ID = cscl_data[key].physicalID
        year = d[3]

        new_key = ID + "-" + year

        return (new_key, record[1])
   

# writes data csv 
# unpacks value tuples
def toCSVLine(data):
  return ','.join(str(e) for e in data)

if __name__ == "__main__":

  from pyspark import SparkContext
  sc = SparkContext()
  import sys

  output_location = sys.argv[1]

  violation_data_file_location = "hdfs:///tmp/bdm/nyc_parking_violations/"

  cscl_data = sc.broadcast(readCenterLineData()).value

  rdd = sc.textFile(violation_data_file_location)

  counts = rdd.mapPartitionsWithIndex(processViolations) \
    .map(lambda data: mapToCenterLineData(data, cscl_data)) \
    .map(toCSVLine) \
    .saveAsTextFile(output_location)

  print('done processing!')