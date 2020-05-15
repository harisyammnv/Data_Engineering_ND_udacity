import requests
import pandas as pd
from bs4 import BeautifulSoup


def tableDataText(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """

    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.findAll(coltag)]

    rows = []
    trs = table.findAll('tr')
    headerow = rowgetDataText(trs[0], 'th')
    if headerow:  # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs:  # for every table row
        rows.append(rowgetDataText(tr, 'td'))  # data row
    return rows


URL = 'https://fam.state.gov/fam/09FAM/09FAM010205.html'
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')
htmltables = soup.find_all("table")
table_names = ['visa-issuing-ports.csv','nationality-codes.csv','port-of-entry-codes.csv']
for tbl,name in zip(htmltables, table_names):
    list_table = tableDataText(tbl)
    df = pd.DataFrame(list_table[1:], columns=list_table[0])
    df.to_csv('C:/Users/harisyam/Desktop/data-engineering-nanodegree/6-Capstone_Project/data/'+name, index=False)