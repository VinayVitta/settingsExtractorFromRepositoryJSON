import pandas as pd
import win32com.client as win32
import os

# Sample data
data = {
    'Region': ['North', 'South', 'East', 'West', 'North'],
    'Product': ['A', 'B', 'C', 'D', 'A'],
    'Sales': [100, 200, 150, 300, 120]
}
df = pd.DataFrame(data)
file = 'pivot_slicer.xlsx'
df.to_excel(file, sheet_name='Data', index=False)

# Open Excel and setup pivot
excel = win32.gencache.EnsureDispatch('Excel.Application')
excel.Visible = True  # Set to False to run in background
wb = excel.Workbooks.Open(os.path.abspath(file))
ws = wb.Sheets('Data')

# Add a new worksheet for Pivot
pivot_ws = wb.Sheets.Add()
pivot_ws.Name = 'PivotTableSheet'

# Define pivot range
last_row = len(df) + 1
pivot_range = ws.Range(f"A1:C{last_row}")

# Create Pivot Cache
pivot_cache = wb.PivotCaches().Create(
    SourceType=1,  # xlDatabase
    SourceData=pivot_range
)

# Create Pivot Table
pivot_table = pivot_cache.CreatePivotTable(
    TableDestination=pivot_ws.Range("A3"),
    TableName="SalesPivot"
)

# Add Pivot Fields
pivot_table.PivotFields("Region").Orientation = 1  # xlRowField
pivot_table.PivotFields("Product").Orientation = 2  # xlColumnField
pivot_table.PivotFields("Sales").Orientation = 4  # xlDataField

# Add Slicer for 'Region'
slicer_cache = wb.SlicerCaches.Add2(pivot_table, 'Region')
slicer = slicer_cache.Slicers.Add(pivot_ws, None, 'Region', 'Region', 300, 100, 144, 200)

# Save and clean up
wb.Save()
# excel.Quit()  # Uncomment to close Excel automatically
