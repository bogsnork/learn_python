# creates a lookup to map the timeID variable to time periods

import pandas as pd

# print(len(list(range(20, -201, -1))))
# print(len([1900] + list(range(1800, -20101, -100))))
# print(len([1900] + [year + 99 for year in range(1800, -20101, -100)]))
# print(len([round(i * 0.1, 1) for i in range(0, -221, -1)]))

# Manually define the data to match the original table
data = {
    "timeID": list(range(20, -201, -1)),
    "startyear": [1900] + list(range(1800, -20101, -100)),
    "endyear": [1990] + [year + 99 for year in range(1800, -20101, -100)],
    "ka B1990": [round(i * 0.1, 1) for i in range(0, -221, -1)],
    "years BP": [35]+[round(i * 100, 1) for i in range(-1, -221, -1)]
}

# Create the DataFrame
df = pd.DataFrame(data)
print(df)
# Save the DataFrame to a CSV file
df.to_csv("Python/holocene_climate/data/time_periods.csv", index=False)


