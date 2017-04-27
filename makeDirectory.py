# Used to process HTTP requests
import requests

# To create DataFrame objects and merge the two Data Frame objects together
import pandas as pd

# Used for web scraping/parsing HTML data
from bs4 import BeautifulSoup

# Used for parallel processing
from joblib import Parallel, delayed

# Also used for parallel processing
import multiprocessing

# Issues an HTTP GET request to the site with the staff directory
iakss = requests.get('http://www.fcps.net/administration/staff-directory-all')

# Parses the content
soup = BeautifulSoup(iakss.content, 'html.parser')

# Extracts all rows from the table of data from the page
rows = [ cell.find_all('td') for cell in soup.find_all('tr') ] 

# Initializes container object to store the resulting dicts
temp = []

# Iterate over each row of the table on the page
for i in rows:

    # Create a new dict object to store the appropriate key value pairs
    record = {}

    # Iterate over the cells in the table object
    for j in i:

        # First object either contains nothing or a link
        if (i.index(j) == 0) & (j.find('a') != None):

            # Gets the <a href="...">something</a> tag from the cell
            link = j.find('a')

            # Adds a link element to the dict and populates it with the href attribute
            record['link'] = link['href']

            # Adds a name element to the dict and populates it with the text displayed for the link
            record['name'] = link.get_text()

        # If the next cell is not empty
        elif (i.index(j) == 1) & (j.get_text() != ''):

            # It contains the job title and gets added to the dict
            record['title'] = j.get_text()

        # The next cell contains the staff members department
        elif i.index(j) == 2:

            # Which is also added to the dict object
            record['department'] = j.get_text()

        # Lastly...
        else:

            # Get the phone number for this staff member
            record['phone'] = j.get_text()

    # If there are a total of 5 elements in the dict object
    if len(record) == 5:

        # Add it to the container used to store the data temporarily
        temp.append(record)


# Convert the array of dict objects into a pandas data frame
data = pd.DataFrame(temp)

# Get a bit more beef out of the local system to process the next batch of calls
cores = multiprocessing.cpu_count() * 2

# Defines a function to process the email web page urls 
# returns a dict object containing all of the information from the individual sites
def getEmailData(url):
    
    # Defines the return object and populates the link to merge back with data object
    returnObject = { 'link' : url }
    
    # Gets the HTML from the site with the email address
    emailSite = requests.get(url).content

    # Gets all of the cells from the table containing the headers and the data elements
    mailSoup = [ cells.find_all('td') for cells in BeautifulSoup(emailSite, 'html.parser').find_all('tr') ]

    # Loops over each of the cells
    for i in mailSoup:

        # Adds a new key value pair to the dict object based on the table layout of the web page
        returnObject[i[0].get_text().replace('\n', '')] = i[1].get_text().replace('\n', '')

    # Returns the dict object
    return returnObject

# Iterate over all of the individual staff member directory pages in parallel
# for each page call the getEmailData() function on the url
# These data get accumlated into an array of dict objects
emailAddresses = Parallel(n_jobs = cores)(delayed(getEmailData)(i) for i in data['link'])

# Now the email addresses get converted into a pandas data frame object
emails = pd.DataFrame(emailAddresses)

# And then get joind to the existing directory data
staffDirectory = pd.merge(data, emails)

# Write the data to a csv file
staffDirectory.to_csv("/Users/billy/Desktop/Programs/Python/fcpsStaffDirectory/directory.csv")

