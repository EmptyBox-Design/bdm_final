def compareStreets(violation_street_name, center_full_street, center_street_label):
  if(violation_street_name == center_full_street or violation_street_name == center_street_label):
    return True
  else:
    return False
    
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

def compareViolationAgainstCenterlines(centerlineData, violation):

  match = None
  
  violation_street_name = violation[2].lower()
  
  violation_year = violation[0]

  # loop through centerline data and see violation returns a match
  for centerLineRow in centerlineData:
    ID = centerLineRow[2]
    # does violation contain data
    # checks if the violation matches either the Full Street Name or Street Label
    if(compareStreets(violation_street_name, centerLineRow[7].lower(), centerLineRow[5].lower())):

      hn = violation[1]
      # checks for empty string
      # if hn:
      try:
        # house number is an integer
        violation_house_number = int(hn)

        if((violation_house_number % 2) == 0):
            
          # house number is even
          c_low = centerLineRow[3]
          c_high = centerLineRow[4]
            
          try:
            # checks if violation house number is greater or equal to min
            # checks if violation house number is less than or equal to max
            if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
              # print('match')
              # this means that the street name of full street name
              # matched and house number is in range
              match = ID
          except ValueError:
            # centerline data is a compound address
            # returns True or False based on match
            if(compareTupes(c_low, c_high, violation_house_number)):
              match = ID

        else:
          # house number is odd
          c_low = centerLineRow[0]
          c_high = centerLineRow[1]

          try:
            if(violation_house_number >= int(c_low) and violation_house_number <= int(c_high)):
              # this means that the street name of full street name
              # matched and house number is in range
              match = ID
          except ValueError:
            # centerline data is a compound address
            # returns True or False based on match
            if(compareTupes(c_low, c_high, violation_house_number)):
              match = ID
      # the violation house number is a tuple or at least not a numerical integer               
      except ValueError:
        # the violation hosue number is not a integer
        # need to split it and treat it as a tuple
        # checking the second value for even or odd
        try:
          if((int(hn.split("-")[1]) % 2) == 0):
            # violation house number is even
            c_low = centerLineRow[3]
            c_high = centerLineRow[4]

            if(compareTupes(c_low, c_high, hn)):
              match = centerLineRow[2]
          else:
            c_low = centerLineRow[0]
            c_high = centerLineRow[1]

            if(compareTupes(c_low, c_high, hn)):
              match = centerLineRow[2]
                      
        except IndexError:
          match = None
        except ValueError:
          match = None


  # returning the centerline physical id and year of violation
  if(match is not None):
    return (match, violation_year)
  else:
    return (match, None)

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
  rows = [] 

  # reading csv file 
  with open(filename, 'r') as csvfile: 
    # creating a csv reader object 
    csvreader = csv.reader(csvfile) 

    # extracting field names through first row 
    fields = next(csvreader) 

    # extracting each data row one by one 
    for row in csvreader:
      if all(len(v) > 0 for v in [row[0], row[1], row[3], row[4], row[5], row[13]]):
        if(len(row[10]) > 0 or len(row[28]) > 0):
          data_required = [row[0], row[1], row[3], row[4], row[5], row[10], row[13], row[28]]
          given_row = [i for i in data_required]
          rows.append(given_row)

    return rows

# converts violation county code abbreviation to 
# centerline code  1 - 5
def getCounty(county):
  county_dict =[
    ['MAN', 'MH', 'MN', 'NEWY', 'NEW', 'Y', 'NY'], 
    ['BRONX', 'BX', 'PBX'], 
    ['BK', 'K', 'KING', 'KINGS'],
    ['Q', 'QN', 'QNS', 'QU', 'QUEEN'],
    ['R', 'RICHMOND', 'ST']]

  i = 0
  while i < len(county_dict):
    if county.upper() in county_dict[i]:
      return i+1
    i += 1

def processViolations(pid, records):
    
  import csv
  ## returns array of centerline rows
  centerline_data = readCenterLineData()

  counts = {}

  if(pid == 0):
    next(records)

  for row in records:
    if all(len(v) > 0 for v in [row[4], row[23], row[24], row[21]]):

      try:
        year = row[4].split("/")[2]
      except IndexError:
        year = '9999'

      house_number = row[23]

      street_name = row[24]

      county = getCounty(row[21])

      violation_row = [year, house_number, street_name, county]

      # print("Violation row: ",violation_row)
      compare = compareViolationAgainstCenterlines(centerline_data, violation_row)

      # only captures violations with matched violations
      if(compare[0] is not None and compare[1] is not None):
        # create physicalID - year
        key = str(compare[0]) + "-" + str(compare[1])
        counts[key] = counts.get(key, 0) + 1
      # elif(compare[0] is not None):
      #   key = str(compare[0]) + "-None"
      #   counts[key] = counts.get(key, 0) +0

  return counts.items()

# writes data csv 
# unpacks value tuples
def toCSVLine(data):
  return ','.join(str(e) for e in data)

if __name__ == "__main__":

  from pyspark import SparkContext
  import sys

  output_location = sys.argv[1]

  violation_data_file_location = "hdfs:///tmp/bdm/nyc_parking_violations/"

  sc = SparkContext()
  rdd = sc.textFile(violation_data_file_location)

  counts = rdd.mapPartitionsWithIndex(processViolations) \
      .reduceByKey(lambda x,y: x+y) \
      .map(toCSVLine) \
      .saveAsTextFile(output_location)
