# CPS-Employee-Position-DataFrame
A cleaned and merged Python, pickled pd.DataFrame of https://cps.edu/About_CPS/Financial_information/Pages/EmployeePositionFiles.aspx

To use in Python:
```python
os.system('wget https://github.com/ahuang11/CPS-Employee-Position-DataFrame/blob/master/EmployeePositionRoster_Joined_Cleaned.pkl?raw=true')
df = pd.read_pickle('EmployeePositionRoster_Joined_Cleaned.pkl')
```

If you decide to use the files in the csv directory, note that they are not fully cleaned and that you may have to run the clean_joined_df function from process_raw.py.
