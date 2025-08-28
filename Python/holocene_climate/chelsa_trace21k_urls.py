import csv

# Variables that have a 'month' component in their filename and URL
MONTHLY_VARS = {"pr", "tasmax", "tasmin"}
BIO_VARS = {"bio01", "bio02", "bio03", "bio04", "bio05", "bio06", "bio07", "bio08", "bio0", "bio10", 
            "bio11", "bio12", "bio13", "bio14", "bio15", "bio16", "bio17", "bio18", "bio19"}

# Map for non-monthly variables to their subfolder (bio, dem, scd, glz, etc.)
NON_MONTHLY_SUBFOLDER = {
    #**{f"bio{str(i).zfill(2)}": "bio" for i in range(1, 19)},
    "dem": "dem",
    "scd": "scd",
    "glz": "glz"
}

def generate_urls_to_query_csv(
    variable="pr",
    months=range(1, 13),
    timeIDs=range(-200, 21),
    output_file="Python/holocene_climate/data/urls_to_query.csv"
):
    """
    Generate URLs for CHELSA TraCE21k GeoTIFFs and write them to a CSV file.
    Handles both monthly and non-monthly variables.
    """
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "month", "timeID", "url"])
        if variable in MONTHLY_VARS:
            base_url = f"https://os.zhdk.cloud.switch.ch/chelsav1/chelsa_trace/{variable}/CHELSA_TraCE21k_{variable}_"
            for month in months:
                for timeID in timeIDs:
                    url = f"{base_url}{month}_{timeID}_V1.0.tif"
                    writer.writerow([variable, month, timeID, url])
        elif variable in BIO_VARS:
            base_url = f"https://os.zhdk.cloud.switch.ch/chelsav1/chelsa_trace/bio/CHELSA_TraCE21k_{variable}_"
            for timeID in timeIDs:
                url = f"{base_url}{timeID}_V1.0.tif"
                writer.writerow([variable, "", timeID, url])        
        else:
            # Determine subfolder for non-monthly variable
            subfolder = NON_MONTHLY_SUBFOLDER.get(variable, variable)
            base_url = f"https://os.zhdk.cloud.switch.ch/chelsav1/chelsa_trace/{subfolder}/CHELSA_TraCE21k_{variable}_"
            for timeID in timeIDs:
                url = f"{base_url}{timeID}_V1.0.tif"
                writer.writerow([variable, "", timeID, url])
    print(f"Wrote URLs to {output_file}")

if __name__ == "__main__":
    # Example usage: generate for pr, tasmax, bio18, dem, glz
    variables = ["bio01", "bio02", "bio03", "bio04", "bio05", "bio06", "bio07", "bio08", "bio0", "bio10", 
            "bio11", "bio12", "bio13", "bio14", "bio15", "bio16", "bio17", "bio18", "bio19"]#["pr", "tasmax", "bio18", "dem", "glz"]
    for var in variables:
        if var in MONTHLY_VARS:
            generate_urls_to_query_csv(
                variable=var,
                months=range(1, 13),
                timeIDs=range(-200, 21),
                output_file=f"Python/holocene_climate/data/urls_to_query_{var}.csv"
            )
        else:
            generate_urls_to_query_csv(
                variable=var,
                months=[],
                timeIDs=range(-200, 21),
                output_file=f"Python/holocene_climate/data/urls_to_query_{var}.csv"
            )