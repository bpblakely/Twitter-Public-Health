import numpy as np
import pandas as pd
import os
from velocity_utils import *
import twitter_filtering_hackathon as filt_funcs
import brian_utils as btils
import ray
import datetime

names = filt_funcs.names
city_filters = filt_funcs.city_filters


@ray.remote
def filter_stuff(region,date,j):
    print(region,date)
    mappings = {}
    rates = []
    try:
        df = read_tweets(filename_builder(date,region),dtype={'full_text':str,'tweet_id':np.int64,'user_location':str,'quer':int})
        df = df.loc[df.retweet == 0].reset_index(drop=True) # don't consider retweets
        df = df.drop(['retweet','original_location','original_date','original_tweet_id','quer','result_type'],axis=1)
        if 'qquery' in df.columns:
            df = df.drop(['qquery'],axis=1)
    except:
        for name in names:
            mappings[name] = pd.DataFrame()
        return mappings, [[]]
    num_tweets = len(df)
    df = filt_funcs.preprocess_corpus(df,region)
    
    # add some more data
    df['seconds_adjusted'] = df['seconds'] + 86400*(j)
    df['day'] = (j+1)
    df['weekday'] = datetime.datetime.strptime(date,"%Y-%m-%d").strftime('%A')

    # organize columns
    t_date = df.pop('tweet_date')
    t_day = df.pop('day')
    t_weekday = df.pop('weekday')
    t_region = df.pop('region')
    df.insert(0,'weekday',t_weekday)
    df.insert(0,'day',t_day)
    df.insert(0,'date',t_date)
    df.insert(0,'region',t_region)
    
    del t_date,t_day,t_weekday,t_region
    
    dff = filt_funcs.do_filter(df)
    counts = []
    for name in names:
        mappings[name] = dff.loc[dff[name] == 1]
        counts.append(len(dff.loc[dff[name] == 1]))
    rates.append([region,date,len(df),num_tweets] + counts)
    del df,dff
    return mappings,rates

regions = ['Atlanta-Sandy Springs-Roswell, GA','Phoenix-Mesa-Scottsdale, AZ','Chicago-Naperville-Elgin, IL-IN-WI',
           'Pittsburgh, PA','Baltimore-Columbia-Towson, MD','Columbus, OH']

# regions = ['Atlanta-Sandy Springs-Roswell, GA','Baltimore-Columbia-Towson, MD','Chicago-Naperville-Elgin, IL-IN-WI']


dates = btils.generate_dates_range('2020-09-22','2021-03-21')
# dates = btils.generate_dates_range('2020-10-31','2020-11-12')
n = len(dates)
    
# chunk_length = 14
# indicies = np.append(np.arange(0,n,chunk_length),[n])


ray.init(num_cpus=13)
final_directory = r'E:/tweets_by_region_filtered_2/'

if __name__ == "__main__":
    for region in regions:
        mappings = {}
        for name in names:
            mappings[name] = pd.DataFrame()
        rates = []
        result = [filter_stuff.remote(region, dates[i],i) for i in range(n)]
        r= ray.get(result)
        for j in range(n):
            for name in mappings:
                mappings[name] = mappings[name].append(r[j][0][name],ignore_index=True).sort_values(by='date').reset_index(drop=True)
            rates.append(r[j][1][0])
                
        for name in names:
            output_path = final_directory + region + '/' + name + '.csv'
            mappings[name].to_csv(output_path,index=False)
        rates = pd.DataFrame(rates,columns=['region','date','count','total_tweets']+names)
        rates.to_csv(final_directory + region + '/counts.csv' ,index=False)
        
        # for i in range(1,len(indicies)):
        #     m = len(dates[indicies[i-1]:indicies[i]])
        #     result = [filter_stuff.remote(region, date,indicies[i-1]+z) for z,date in enumerate(dates[indicies[i-1]:indicies[i]])]
        #     r= ray.get(result)
        #     for j in range(m):
        #         for name in mappings:
        #             mappings[name] = mappings[name].append(r[j][0][name],ignore_index=True).sort_values(by='date').reset_index(drop=True)
        #         rates.append(r[j][1][0])
        #     for name in names:
        #             output_path = final_directory + region + '/' + name + '.csv'
        #             mappings[name].to_csv(output_path,mode='a',index=False,header=not os.path.exists(output_path))
        #             mappings[name] = pd.DataFrame()
        # rates = pd.DataFrame(rates,columns=['region','date','count','total_tweets']+names)
        # rates.to_csv(final_directory + region + '/counts.csv' ,index=False)


atl = pd.read_csv(final_directory + 'Atlanta-Sandy Springs-Roswell, GA/counts.csv')
phx = pd.read_csv(final_directory + 'Phoenix-Mesa-Scottsdale, AZ/counts.csv')
chicago = pd.read_csv(final_directory + 'Chicago-Naperville-Elgin, IL-IN-WI/counts.csv')
pitts = pd.read_csv(final_directory + 'Pittsburgh, PA/counts.csv')
balt = pd.read_csv(final_directory + 'Baltimore-Columbia-Towson, MD/counts.csv')
columbus = pd.read_csv(final_directory + 'Columbus, OH/counts.csv')


atl.total_tweets.sum() + phx.total_tweets.sum() +chicago.total_tweets.sum() + pitts.total_tweets.sum() + balt.total_tweets.sum() + columbus.total_tweets.sum()

#%%

def simplified_read(file):
    return pd.read_csv(file,parse_dates=['date'],usecols=['date','day','scrubbed_text','retweet_count','favorite_count','friends_count','verified','days_since_creation'])

df = pd.read_csv(final_directory + 'Columbus, OH/covid.csv',parse_dates=['date'],usecols=['date','day','scrubbed_text','retweet_count','favorite_count','friends_count','verified','days_since_creation'])
a = df.groupby([pd.Grouper(key='date', freq='W-MON')]).count()


df = pd.read_csv(final_directory + 'Columbus, OH/treatments.csv',parse_dates=['date'],usecols=['date','day','scrubbed_text','retweet_count','favorite_count','friends_count','verified','days_since_creation'])
a = df.groupby([pd.Grouper(key='date', freq='W-MON')]).count()

b= df.groupby([pd.Grouper(key='date', freq='W-MON')])['day'].count().rename('count')
#%%
def create_weekly_data(region):
    weekly_df = pd.DataFrame()
    for name in names:
        file_in = final_directory + region + '/'+ name + '.csv'
        df = simplified_read(file_in)
        a = df.groupby([pd.Grouper(key='date', freq='W-MON')])['day'].count().rename(name)
        weekly_df = weekly_df.append(a)
    return weekly_df

weekly_df.T.plot(xlabel='Date',ylabel = 'Tweet Frequency', title='Tweet Topic Frequency Over Time')

weekly_df.div(weekly_df.sum(axis=1), axis=0).T.plot(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title='Correlation Between Tweet Topics Over Time')


#%%
import seaborn as sns
import matplotlib.pyplot as plt


sns.set_theme()
dataframes = []
for region in regions:
    weekly_df = create_weekly_data(region)
    dataframes.append(weekly_df)
    normalized = weekly_df.div(weekly_df.sum(axis=1), axis=0).T
    test = normalized.rename_axis('date').reset_index()
    
    fig, ax = plt.subplots(1)

    for name in names:
        ax = sns.lineplot(data=test,x='date',y=name,label=name)
    ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title=f'Correlation Between Tweet Topics Over Time\n{region}')
    fig.set_size_inches(8.75, 7.5)
    fig.tight_layout()
    fig.savefig(region+'_lineplot.png',dpi=800)
    fig.clf()

#%% Sentiment
# from textblob import TextBlob

# weekly_df_s = pd.DataFrame()
# for name in names:
#     file_in = final_directory + region + '/'+ name + '.csv'
#     df = simplified_read(file_in)
#     df['sentiment']= df.scrubbed_text.apply(lambda x: TextBlob(x).sentiment.polarity)
#     df['topic'] = name
#     weekly_df_s = weekly_df_s.append(df)

# weekly_df_s['sentiment'] = weekly_df_s['sentiment'].astype(np.float32)

# weekly_df_s = weekly_df_s.groupby(['topic',pd.Grouper(key='date', freq='W-MON')])['sentiment'].mean().unstack().T
# weekly_df_s = weekly_df_s.rename_axis('date').reset_index()

# weekly_df_s.plot(x='date')

# for name,g in b.groupby('topic'):
#     sns.lineplot(data=g)

# weekly_df_s.groupby([pd.Grouper(key='date', freq='W-MON')])['sentiment'].mean()

# weekly_df_s = weekly_df_s.T
# weekly_df_s = weekly_df_s.rename_axis('date').reset_index()
    

# weekly_df_s.plot(x='date')

# df = pd.read_csv(final_directory + 'Columbus, OH/covid.csv',parse_dates=['date'],usecols=['date','day','scrubbed_text','retweet_count','favorite_count','friends_count','verified','days_since_creation'])

# sentiment = df.scrubbed_text.apply(lambda x: TextBlob(x).sentiment)
#%% Better Sentiment
from nltk.sentiment import SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()

weekly_df_s = pd.DataFrame()
for name in names:
    file_in = final_directory + region + '/'+ name + '.csv'
    df = simplified_read(file_in)
    df['sentiment']= df.scrubbed_text.apply(lambda x: sia.polarity_scores(x)['compound'])
    df['topic'] = name
    #a = df.groupby([pd.Grouper(key='date', freq='W-MON')])['sentiment'].mean().rename(name)
    weekly_df_s = weekly_df_s.append(df)

weekly_df_s['sentiment'] = weekly_df_s['sentiment'].astype(np.float32)


weekly_df_std = weekly_df_s.groupby(['topic',pd.Grouper(key='date', freq='W-MON')])['sentiment'].std().unstack().T
weekly_df_std = weekly_df_std.rename_axis('date').reset_index()

weekly_df_std.plot(x='date')

#%% Simple frequency plot

for name in names:
    ax = sns.lineplot(data=test,x='date',y=name,label=name)
    
ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title='Correlation Between Tweet Topics Over Time')
plt.show()

#%% Frequency plot with controversy
from matplotlib.patches import Patch
sns.set_theme()

dataframes = []
tests = []
for region in regions:
    weekly_df = create_weekly_data(region)
    dataframes.append(weekly_df)
    normalized = weekly_df.div(weekly_df.sum(axis=1), axis=0).T
    test = normalized.rename_axis('date').reset_index()
    tests.append(test)
#%% Controversy
for i,region in enumerate(regions):
    fig, ax = plt.subplots(1)
    test = tests[i]
    for name in names:
        ax.plot(test['date'],test[name],label=name)
        ax.fill_between(test['date'], (test[name]+test[name]*weekly_df_std[name]*.5), (test[name]-test[name]*weekly_df_std[name]*.5), alpha=0.3)
    ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title=f'Tweet Topics Frequency and Controversy Over Time\n{region}')
    
    handles, labels = ax.get_legend_handles_labels()
    handles.append(Patch(facecolor='grey', edgecolor='black',label='controversy'))
    fig.legend(handles = handles)
    fig.set_size_inches(8.75, 7.5)
    fig.tight_layout()
    fig.savefig(region+'_polarity.png',dpi=800)
    fig.clf()
    
#%% Controversy Constrained
fig, axes = plt.subplots(nrows = 3, ncols = 2,constrained_layout=True)
i = 0
j = 0 
for region in regions:
    ax = axes[i][j]
    test = tests[i]
    for name in ['covid','masks','treatments','social_distancing']:
        ax.plot(test['date'],test[name],label=name)
        ax.fill_between(test['date'], (test[name]+test[name]*weekly_df_std[name]*.5), (test[name]-test[name]*weekly_df_std[name]*.5), alpha=0.3)
    ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title = region)
    
    handles, labels = ax.get_legend_handles_labels()
    handles.append(Patch(facecolor='grey', edgecolor='black',label='controversy'))
    i += 1 
    if i >=3: i=0 
    if j == 0: j=1
    elif j == 1: j=0
fig.suptitle(f'Tweet Topics Frequency and Controversy Over Time by Region')
fig.legend(handles = handles)

fig.set_size_inches(8.75, 7.5)
fig.tight_layout()
fig.savefig(region+'_polarity.png',dpi=800)
fig.clf()
#%%
fig, ax = plt.subplots(1)
for name in names:
    ax.plot(test['date'],test[name],label=name)
    ax.fill_between(test['date'], (test[name]+test[name]*weekly_df_std[name]*.6), (test[name]-test[name]*weekly_df_std[name]*.6), alpha=0.3)
ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title='Tweet Topics Frequency and Controversy Over Time')

handles, labels = ax.get_legend_handles_labels()
handles.append(Patch(facecolor='grey', edgecolor='black',label='controversy'))
fig.legend(handles = handles).set_draggable(True)

#%%
fig, ax = plt.subplots(1)
for name in ['covid','masks','treatments','social_distancing']:
    ax.plot(test['date'],test[name],label=name)
    ax.fill_between(test['date'], (test[name]+test[name]*weekly_df_std[name]*.6), (test[name]-test[name]*weekly_df_std[name]*.6), alpha=0.3)
ax.set(xlabel='Date',ylabel = 'Normalized Tweet Frequency', title='Tweet Topics Frequency and Controversy Over Time')

handles, labels = ax.get_legend_handles_labels()
handles.append(Patch(facecolor='grey', edgecolor='black',label='controversy'))
fig.legend(handles = handles).set_draggable(True)

#%%
import emoji
import re
covid = pd.read_csv(r'G:\Python File Saves\covid_us.csv')


emojis_list_de= re.findall(r'(:[!_\-\w]+:)', emoji.demojize(df.full_text.iloc[3]))

def extract_emojis(sentence):
    return [word for word in sentence.split() if str(word.encode('unicode-escape'))[2] == '\\' ]

extract_emojis(df.full_text.iloc[3])
regex.findall(r'[^\w\s,]', df.full_text.iloc[3])
emoji.demojize(df.full_text.iloc[3])

df = pd.read_csv(file_in,parse_dates=['date'],usecols=['date','day','full_text','scrubbed_text','retweet_count','favorite_count','friends_count','verified','days_since_creation'])

a = df.full_text.apply(lambda x: re.findall(r'(:[!_\-\w]+:)', emoji.demojize(x))).apply(lambda x: [emoji.emojize(e) for e in x])
df['emojis'] = a

def emoji_counts(unicodes):
    if len(unicodes) == 0 or unicodes is None: return 
    word_counts = dict()
    for i in range(len(unicodes)):
        if unicodes[i] not in word_counts:
            word_counts[unicodes[i]]= 1
        else:
            word_counts[unicodes[i]]+=1
    return max(word_counts, key=word_counts.get)

def build_emoji_list(df):
    li = []
    df.emojis.dropna().apply(lambda x: li.extend(x) if x != None else [])
    return emoji_counts(li)

df.dropna().groupby(pd.Grouper(key='date', freq='W-MON')).agg(build_emoji_list)['emojis']

#%%
data = []
for region in regions:
    d = pd.DataFrame()
    for name in names:
        file_in = final_directory + region + '/'+ name + '.csv'
        df = simplified_read(file_in)
        d = d.append(df)
    data.append(d)

#%%
def creation_classifer(days):
    if days <= 10: return '<=10'
    elif days <= 30: return '<=30'
    elif days <= 50: return '<=50'
    elif days <= 100: return '<=100'
    elif days <= 500: return '<=500'
    else: return '>500'
# [(lambda x, y : '<='+str(x) if y <= x else None)(day, 10) for day in [10,30,50,100,500]]


df['account_age'] = df.days_since_creation.apply(creation_classifer)
df.groupby('account_age')['date'].count().plot.pie(ylabel='Days Since Creation',title='Account Age for ' +name)
#%% Exploring Account Age Statistics
fig, axes = plt.subplots(nrows = 3, ncols = 2,constrained_layout=True)
i = 0
j = 0 
for name in ['covid','masks','treatments','social_distancing','symptoms','working']:
    ax = axes[i][j]
    test = data[i]
    test['account_age'] = test.days_since_creation.apply(creation_classifer)
    # ax.pie(x=test.groupby('account_age')['date'].count(), labels=['']*6,
    #                                                      autopct="%.1f%%",explode=[0.05]*6,pctdistance=0.4,textprops={'fontsize': 9})
    test.groupby('account_age')['date'].count().plot.pie(ylabel='Account Age',title=name,ax = ax, labels=['']*6,
                                                         explode=[0.05]*6,textprops={'fontsize': 9})
    i += 1 
    if i >=3: i=0 
    if j == 0: j=1
    elif j == 1: j=0
fig.suptitle(f'Tweet Topics Frequency and Controversy Over Time for {region}')
fig.legend(list(test.groupby('account_age')['date'].count().index),title='Days Old')
