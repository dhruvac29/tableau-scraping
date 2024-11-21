from tableauscraper import TableauScraper as TS
import pandas as pd
import time

url = "https://public.tableau.com/views/AskCHISNE2_0/DataTable?%3Adisplay_static_image=n&%3Aembed=true&%3Aembed=y&%3Alanguage=en-US&%3AshowVizHome=n&%3AapiID=host0#navType=0&navSrc=Parse"

# Load the Tableau workbook
ts = TS()
ts.loads(url)
workbook = ts.getWorkbook()

ws = workbook.getWorksheet("DATA TABLE")
# Yr = [2022, 2020, 2018, 2016, 20102015, 2014, 2012]
ws = ws.setFilter("Yr", "", indexValues=[0])

newWS = ws.getWorksheet("Rando")

# Get the filter values for indicators
indicators = [
    t["values"] for t in newWS.getFilters() if t["column"] == "VariableDropDown"
][0]

# Set filters for indicators
indicatorsWB = newWS.setFilter("VariableDropDown", indicators)
indicatorsWB = newWS.setFilter("geoType (AskCHISNEpolygons2016.shp)", "tract")

# Prepare variables for writing to CSV
csv_file = "2022.csv"
header_written = False

for i in range(10200):
    # Set filter with current index value
    indicatorsWB = newWS.setFilter("nameCleanedFINAL", "", indexValues=[i])
    df = indicatorsWB.getWorksheet("DATA TABLE").data

    # Only write to CSV if df is not empty
    if not df.empty:
        # Write to CSV; only include header on first write
        if not header_written:
            df.to_csv(csv_file, index=False, mode="w")  # Write with header
            header_written = True
        else:
            df.to_csv(
                csv_file, index=False, mode="a", header=False
            )  # Append without header
    time.sleep(5000)  # Add delay to avoid overloading the server