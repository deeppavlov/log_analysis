from poller import get_db
from PyPDF2 import PdfMerger
files_collection = get_db('mongo', 27017, 'stat', 'stat', 'pypistat')

from bson.son import SON
def plot_pdf():
    pipeline = [

        {"$unwind": "$country_code"},

        {"$group": {"_id": "$country_code", "count": {"$sum": 1}}},

        {"$sort": SON([("count", -1), ("_id", -1)])}

    ]
    dat = files_collection.find({'details.installer.name': 'pip'})
    dat = [d['country_code'] for d in dat]
    dat = {x: dat.count(x) for x in set(dat)}
    dat = sorted([{'_id': k, 'count': v} for k, v in dat.items()], key= lambda a: -a['count'])
    # data = list(files_collection.aggregate(pipeline))
    data = dat

    import pycountry
    import geopandas
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.DataFrame(data)

    def alpha3code(column):
        CODE=[]
        for country in column:
            try:
                code=pycountry.countries.get(alpha_2=country).alpha_3
               # .alpha_3 means 3-letter country code
               # .alpha_2 means 2-letter country code
                CODE.append(code)
            except:
                CODE.append('None')
        return CODE# create a column for code
    df['CODE']=alpha3code(df._id)


    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))# rename the columns so that we can merge with our data
    world.columns=['pop_est', 'continent', 'name', 'CODE', 'gdp_md_est', 'geometry']
    merge=pd.merge(world,df,on='CODE')# last thing we need to do is - merge again with our location data which contains each country’s latitude and longitude
    location=pd.read_csv('https://raw.githubusercontent.com/melanieshi0120/COVID-19_global_time_series_panel_data/master/data/countries_latitude_longitude.csv')
    merge=merge.merge(location,on='name').sort_values(by='count',ascending=False).reset_index()

    # plot confirmed cases world map
    merge.plot(column='count', scheme="quantiles",k=20,
               figsize=(50, 40),
               legend=True,cmap='coolwarm')
    plt.title('DeepPavlov library pypi downloads since Jan 2019',fontsize=25)# add countries names and numbers
    for i in range(0,10):
        plt.text(float(merge.longitude[i]),float(merge.latitude[i]),"{}\n{}".format(merge.CODE[i],merge['count'][i]),size=10)
    plt.savefig('1.pdf')


    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    start = datetime(year=2019, month=1, day=1)


    def get_stat(st):
        dmounthly = files_collection.find({'timestamp':{'$gte': st, '$lt':st+relativedelta(months=1)},
                                           'details.installer.name': 'pip'})
        dmounthly = [d['country_code'] for d in dmounthly]
        dmounthly = {x: dmounthly.count(x) for x in set(dmounthly)}
        return dmounthly
    now = datetime.now()
    ans = {}
    while start != datetime(year=now.year, month=now.month, day=1):
        stat = get_stat(start)
        ans[start] = stat
        start += relativedelta(months=1)

    import matplotlib.pyplot as plt
    import numpy as np

    # data set
    whitelist = {s['_id']: s['count'] for s in data[:19] if s['_id'] is not None}

    x = sorted(ans.keys())
    bars = {w:[] for w in whitelist.keys()}
    bars['Others'] = []
    for month in x:
        vals = ans[month]
        for country in whitelist:
            bars[country].append(vals.get(country, 0))
        bars['Others'].append(sum([v for k,v in vals.items() if k not in whitelist]))
    bars = {k:np.array(v) for k,v in bars.items()}
    whitelist['Others'] = sum([s['count'] for s in data if s['_id'] not in whitelist])
    # plot stacked bar chart
    # plt.bar(x, y1, color='g')
    # plt.bar(x, y2, bottom=y1, color='y')
    # plt.show()
    order = [k for k in sorted(whitelist, key=lambda k: -whitelist[k])]
    colors = ['#e6194B', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#42d4f4', '#f032e6', '#bfef45', '#fabed4', '#469990', '#dcbeff', '#9A6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1', '#000075', '#a9a9a9', '#ffffff', '#000000']
    bottom = None
    plt.figure(figsize=(50,40))
    plt.rcParams['font.size'] = 35
    xlabels = [i.strftime('%b %Y').replace(' 20','') for i in x]
    for c, col in zip(order, colors):
        plt.bar(x, bars[c], color=col, bottom=bottom, width = 20)
        if bottom is None:
            bottom = bars[c]
        else:
            bottom += bars[c]
    plt.xlabel("Month")
    plt.ylabel("Installations")
    plt.xticks(rotation=45)
    plt.legend(order)
    plt.title("DeepPavlov installations by month")
    plt.savefig('2.pdf')

    fig, ax = plt.subplots()

    df1 = df[['CODE', 'count']]
    #create table
    table = ax.table(cellText=df1.values, colLabels=['Country code', 'Number of Downloads'], loc='center')

    #modify table
    table.set_fontsize(14)
    table.scale(1,4)
    ax.axis('off')

    #display table
    plt.savefig('3.pdf', bbox_inches='tight')

    merger = PdfMerger()
    for i in range(1,4):
        merger.append(f'{i}.pdf')
    from datetime import datetime
    today = datetime.now()
    merger.write(f'report.pdf')
