BASE_URL = 'https://cps.edu'
FILE_URL = ('https://cps.edu/About_CPS/Financial_information/'
            'Pages/EmployeePositionFiles.aspx')


def list_urls():
    """Get a list of urls from the file url."""
    request = requests.get(FILE_URL)
    soup = BeautifulSoup(request.content, 'lxml')
    urls = [os.path.join(BASE_URL, link.attrs['href'].lstrip('/'))
            for link in soup.find_all('a')
            if 'Employee' in link.attrs['href']]
    return urls


def get_paths(urls):
    """Download two files simultaneously and output to raw directory."""
    os.makedirs('raw', exist_ok=True)
    db.from_sequence(urls, npartitions=2).map(
        lambda url: os.system(f'wget -nc {url} -P raw')).compute()
    return sorted(glob.glob(os.path.join('raw', '*.xls')) +
                  glob.glob(os.path.join('raw', '*.pdf')))


def parse_date(path):
    """Parse date into pd.Timestamp by trying three date formats."""
    for fmt in ['%m%d%Y', '%m_%d_%y', '%m-%d-%Y']:
        try:
            # right after Roster is the date
            # :-4 to remove path extension (.xls, .pdf)
            date_str = path.split('Roster')[-1].lstrip('_')[:-4]
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            pass


def postprocess_df(df, date):
    """Make table names consistent across files."""
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace('\n', ' ')
                  .str.replace('\r', ' ')
                  .str.replace('_', ' ')
                  )
    df = df.rename(columns={
        'employee name': 'name',
        'jobcode': 'job code',
        'job description': 'job title',
        'department': 'unit name',
        'pos #': 'position number',
        'dept id': 'unit number',
        'dept/unit name': 'unit name',
        'gross salary': 'annual salary',
        'fte salary': 'fte annual salary',
        'dept/unit number': 'unit number',
        'annual  benefit  cost': 'annual benefit cost'
    })
    df = df.assign(**{'date': date}).set_index('date')
    return df


def read_pdf(path, date):
    """The PDFs' formatting change over time..."""
    if date == pd.datetime(2012, 7, 11):
        skiprows = 0
        lattice = False
    elif date > pd.datetime(2012, 7, 11):
        skiprows = 0
        lattice = True
    else:
        skiprows = 1
        lattice = True

    df = tb.read_pdf(path, pages='all', lattice=lattice,
                     pandas_options=dict(skiprows=skiprows))

    if date == pd.datetime(2012, 7, 11):
        temp_df = df['FTE Salary'].str.split(' ', expand=True)
        df['fte'] = temp_df[0]
        df['annual salary'] = (
            temp_df[1].str.lstrip('$').str.replace(',', ''))
        df['union affiliation'] = (
            temp_df[2]
            .str.cat(temp_df.loc[:, 3:].astype(str), sep=' ')
            .str.replace('None', ' ')
        )
        df = df.drop(columns=['FTE Salary', 'Union Affiliation'])

    elif date == pd.datetime(2010, 7, 1):
        df.columns = [
            'position number', 'budget_category', 'unit number',
            'unit name', 'name', 'job title', 'annual salary',
            'fte', 'union affiliation'
        ]

    return df


def read(path, replace=False):
    """Return a df from postprocessed csv, or postprocess pdf/xls."""
    try:
        date = parse_date(path)
        if date < pd.datetime(2010, 5, 2):
            return None  # pdf tables that aren't easily readable

        os.makedirs('csv', exist_ok=True)
        csv_file = f'EmployeePositionRoster_{date:%m%d%Y}.csv'
        csv_path = os.path.join('csv', csv_file)

        if os.path.exists(csv_path) and not replace:
            df = pd.read_csv(csv_path, index_col='date', parse_dates=True)
        else:
            if path.endswith('.xls'):
                df = pd.read_excel(path)
            elif path.endswith('.pdf'):
                df = read_pdf(path, date)
            df = postprocess_df(df, date)
            df.to_csv(csv_path)

        return df
    except Exception as e:
        print(e, path)


def clean_joined_df(df):
    """Perform any remaining necessary cleanups."""
    df.index = pd.to_datetime(df.index, errors='coerce')
    df = df.loc[~((pd.isnull(df.index)) |
                  (df['position number'] == 'Position Number') |
                  (df['position number'].str.startswith('Chicago Public')) |
                  (df['position number'].str.startswith('POSITION')) |
                  (df['position number'].str.startswith('Position'))
                  )]
    df = df.loc[~pd.isnull(df['position number'])]
    for col in ['annual salary', 'fte annual salary',
                'total position cost', 'annual benefit cost']:
        df.loc[:, col] = (df[col].astype(str)
                          .str.replace(',', '')
                          .str.replace('nan', ''))
        if 'salary' in col:
            df.loc[:, col] = df[col].astype(str).str.replace('$', '')
        if 'cost' in col:
            df.loc[:, col] = df[col].astype(str).str.replace(u'\xa0', '')

    numeric_cols = ['position number', 'unit number', 'fte', 'annual salary',
                    'fte annual salary', 'annual benefit cost', 'job code',
                    'total position cost']
    text_cols = list(set(df.columns) - set(numeric_cols))
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    df[text_cols] = df[text_cols].astype(str).apply(
        lambda col: col.str.replace('nan', ''))
    df = df.sort_index()

    int_cols = ['position number', 'unit number']
    df[int_cols] = df[int_cols].astype(int)
    df = df.drop(columns=[
        'total position cost', 'clsindc', 'fte annual salary',
        'budget category', 'annual benefit cost'])
    return df


def load():
    urls = list_urls()
    paths = get_paths(urls)

    pkl_file = f'EmployeePositionRoster_Joined_Cleaned.pkl'
    if os.path.exists(pkl_file):
        return pd.read_pickle(pkl_file)

    df = pd.concat((read(path) for path in paths), sort=False)
    df = clean_joined_df(df)
    df.to_pickle(pkl_file)
    return df
