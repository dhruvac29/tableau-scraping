from tableauscraper import TableauScraper as TS
import pandas as pd
import time
import random
from multiprocessing import Pool
import os

# URL of the Tableau workbook
url = "https://public.tableau.com/views/AskCHISNE2_0/DataTable?%3Adisplay_static_image=n&%3Aembed=true&%3Aembed=y&%3Alanguage=en-US&%3AshowVizHome=n&%3AapiID=host0#navType=0&navSrc=Parse"


def process_partition(partition_range):
    """
    Process a range of indices in the partition to fetch data from Tableau and write to temporary CSV.
    """
    ts = TS()
    ts.loads(url)
    workbook = ts.getWorkbook()
    ws = workbook.getWorksheet("DATA TABLE")
    ws = ws.setFilter("Yr", "", indexValues=[2])
    newWS = ws.getWorksheet("Rando")

    # Set filters for indicators
    indicators = [
        t["values"] for t in newWS.getFilters() if t["column"] == "VariableDropDown"
    ][0]
    indicatorsWB = newWS.setFilter("VariableDropDown", indicators)
    indicatorsWB = newWS.setFilter("geoType (AskCHISNEpolygons2016.shp)", "tract")

    temp_csv_file = f"temp_partition_{partition_range[0]}_{partition_range[-1]}.csv"
    header_written = False

    for i in partition_range:
        try:
            # Set filter with the current index value
            indicatorsWB = newWS.setFilter("nameCleanedFINAL", "", indexValues=[i])
            df = indicatorsWB.getWorksheet("DATA TABLE").data

            # Only write to CSV if df is not empty
            if not df.empty:
                # Write to temporary CSV; only include header on first write
                if not header_written:
                    df.to_csv(temp_csv_file, index=False, mode="w")  # Write with header
                    header_written = True
                else:
                    df.to_csv(
                        temp_csv_file, index=False, mode="a", header=False
                    )  # Append without header

            # Add delay to avoid overloading the server
            time.sleep(random.randint(1, 2))
        except Exception as e:
            print(f"Error processing index {i}: {e}")

    return temp_csv_file


def combine_temp_csvs(output_csv_file, num_partitions, partition_size, total_indices):
    """
    Combine all temporary partition files into a single CSV file in sequence.
    """
    temp_files = [
        f"temp_partition_{i * partition_size}_{(i + 1) * partition_size - 1}.csv"
        for i in range(num_partitions)
    ]
    temp_files[-1] = (
        f"temp_partition_{(num_partitions - 1) * partition_size}_{total_indices - 1}.csv"  # Adjust for last range
    )

    df_list = []
    for file in temp_files:
        if os.path.exists(file):
            df_list.append(pd.read_csv(file))
        else:
            print(f"Warning: Missing file {file}, skipping.")

    # Combine all dataframes and write to the final CSV
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv(output_csv_file, index=False)

    # Clean up temporary files
    for file in temp_files:
        try:
            os.remove(file)
        except Exception as e:
            print(f"Error deleting file {file}: {e}")


def main():
    total_indices = 10200
    num_partitions = 10
    partition_size = total_indices // num_partitions

    # Create partitions
    partitions = [
        range(i * partition_size, (i + 1) * partition_size)
        for i in range(num_partitions)
    ]
    partitions[-1] = range(
        (num_partitions - 1) * partition_size, total_indices
    )  # Ensure all indices are covered

    # Use multiprocessing to handle partitions in parallel
    with Pool(processes=num_partitions) as pool:
        pool.map(process_partition, partitions)

    # Combine all temporary files into the final CSV
    final_csv_file = "Year_2017_2018.csv"
    combine_temp_csvs(final_csv_file, num_partitions, partition_size, total_indices)
    print(f"Final CSV file generated: {final_csv_file}")


if __name__ == "__main__":
    main()
