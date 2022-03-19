import pandas as pd
import re, glob, os

cols = ['date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query', 's_port', 'cs_username', 'c_ip'
, 'cs_user_agent', 'cs_referer', 'sc_status', 'sc_substatus', 'sc_win32_status', 'time_taken']

logs = glob.glob(os.path.join('.', "logs/*.log"))
df = pd.concat(map(lambda log: pd.read_csv(log, sep=' ', comment='#', engine='python', names=cols), logs))

df['datetime'] = df.apply(lambda row: row.date + ' ' + row.time, axis=1)
df.datetime = pd.to_datetime(df.datetime)
df.datetime = df.datetime.dt.tz_localize('UTC').dt.tz_convert('NZ')

df = df.loc[(df['datetime'] >= '2022-03-18') & (df['datetime'] < '2022-03-19')]

df.to_csv('./output/iis_log.csv')

byapi = {}
byuser = {}
for index, row in df.iterrows():
    api = re.sub('[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?', 'guid', row.cs_uri_stem)
    api = re.sub('([\/][^\/]+)\.(jpg|JPG|jpeg|JPEG|png|PNG|pdf)', 'imgpdf', api)
    api = re.sub('tests\/[A-Z0-9]{8}', 'tests', api)
    if api in byapi.keys():
        byapi[api]['count'] += 1
        byapi[api]['time_total'] += row.time_taken
    else:
        byapi[api] = {'count':1, 'time_total':row.time_taken}

    user = row.cs_username
    if user in byuser.keys():
        byuser[user]['count'] += 1
        byuser[user]['time_total'] += row.time_taken
    else:
        byuser[user] = {'count':1, 'time_total':row.time_taken}

data = []
for x in byapi:
    data.append({'url':x, 'count':byapi[x]['count'], 'time_total':byapi[x]['time_total']/1000, 'time_avg':byapi[x]['time_total']/byapi[x]['count']/1000})

pd_api = pd.DataFrame(data)

pd_api.to_csv('./output/by_api.csv')
print(pd_api.describe())

data = []
for x in byuser:
    data.append({'url':x, 'count':byuser[x]['count'], 'time_total':byuser[x]['time_total']/1000, 'time_avg':byuser[x]['time_total']/byuser[x]['count']/1000})

pd_user = pd.DataFrame(data)

pd_user.to_csv('./output/by_user.csv')
print(pd_user.describe())


