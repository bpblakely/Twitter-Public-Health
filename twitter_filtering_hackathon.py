import numpy as np
import pandas as pd
import os
import warnings
import re
from nltk.tokenize import TweetTokenizer
from functools import partial


warnings.filterwarnings("ignore")

regions = ['Atlanta-Sandy Springs-Roswell, GA','Phoenix-Mesa-Scottsdale, AZ','Chicago-Naperville-Elgin, IL-IN-WI','Columbus, OH',
           'Pittsburgh, PA','Baltimore-Columbia-Towson, MD']

queries = [""""covid*" OR "coronavirus" OR "rona" or "corona virus" or "cv19" """,
           """"virus" OR "flu" OR "pandemic" OR "Fauci" OR "pneumonia" OR "sars" """,
           """"Mask*" OR "PPE" OR "N95" OR "Face Shield" OR (face near/3 cover*)""",
           """"antigen*" or "antibody*" OR "antibodies" OR "seroprevalence" """,
           """"vaccin*" OR "ventilator*" OR "contact tracing" OR "remdesivir" OR "pfizer" OR "moderna" OR "pfszer" """,
           """"disinfect" OR "disinfectant*" OR "lysol" OR "sanitize*" OR "sanitizing" OR "bleach" or "clorox" or (hands near/3 wash*) or (hand near/3 wash*) or (hand near/3 sanitiz*) or (hands near/3 sanitiz*)""",
           """"quarant*" OR "social distanc*" OR "lockdown" or "lock down" OR "hunker* down" OR "6 ft apart" OR "6 feet apart" OR "Six feet apart" """,
           """"Fever" OR "Cough" OR "Chills" OR "asymptomatic" OR "Cant smell" OR "No Smell" OR "Cant taste" OR "No Taste" OR (loss near/3 smell) or (loss near/3 taste) or (sore near/5 throat) or (lost near/3 smell) or (lost near/3 taste)""",
           """"unemployed" OR "stimulus check*" OR "reopen*" OR (work* near/3 home) OR "wfh" OR "furlough" OR "remote work" OR "working remotely" OR "not working now" """]
names = ['covid','general_virus','masks','test','treatments','sanitizing','social_distancing','symptoms','working']


city_filters = {'Atlanta-Sandy Springs-Roswell, GA': 'alph|aretta|pharet|atl|lanta|mari|ietta|sand', 
                'Phoenix-Mesa-Scottsdale, AZ': 'Cas|asa|Gra|rand|ande|tem|tp|mpe|scotts|sdale|ttsd|Cha|ler|handle|Mes|esa|pho|phe|eno|phx|phi|enix', 
                'Chicago-Naperville-Elgin, IL-IN-WI': 'boli|ingbr|Chic|cago|wind|Des|keno|sha|keosh|Elgi|gary|sko|skie|umburg|schaum|Evan|van|Hof|ffman|naper', 
                'Columbus, OH': 'col|umb|bus', 'Pittsburgh, PA': 'pitts|burgh', 'Baltimore-Columbia-Towson, MD': 'balt|imore|blt|columbi|towso|twsn|wson'}

# returns the filtered dataframe
# An example call of the function: filter_city(dataframe,city_filters[regions[0]])
    # city_filters[regions[0]] uses the dictonary to let you provide a region and match it with it's city filter
def filter_city(df,region):
    return df.loc[df.user_location.str.contains(city_filters[region],regex=True,case=False)>0]

def remove_url(string):
    return re.sub(r'http\S+', '', string)

def remove_special(string):
    return re.sub('[^A-Za-z0-9]+', ' ', string)

# lets not prepare tokenizer more than we need to

def prepare_text(string):
    tknzr = TweetTokenizer(strip_handles=True,preserve_case=False) # prepare tokenizer
    string = ' '.join(tknzr.tokenize(string)) # remove all 
    string = remove_url(string) # remove all possible URL's
    string = remove_special(string) # remove all special characters such as #!@%
    # string = string.lower() # convert all text to lowercase, already done with tokenizer
    return string.strip() # remove spaces if any

def star_match(word,keyword):
    return word.startswith(keyword) # stars with, so now we dont get random substring 
def full_match(word,keyword):
    return word == keyword

# use any() to use lazy evaluation
def full_search(string,keyword):
    return keyword in string.split()
    #return any((full_match(w,keyword) for w in string.split(' ')))

def star_search(string,keyword):
    return any((star_match(w,keyword) for w in string.split()))

# might be slower than previous version, but its WAY more accurate
# def phrase_search(string,phrase):
#     # see commented code below for an easier to read version of this code
#     expression = re.compile(r'\b'+ r' '.join([('(' + k +')').replace('*)',')[a-zA-Z]*') for k in phrase.split(' ')]) + r'\b')
#     return bool(expression.search(string))
    
def phrase_search(string,phrase): # Super fast and accurate
    if any((w.replace('*','') not in string for w in phrase.split())): return False # quickly filter out ineligable tweets
    
    if phrase.find('*') >= 0: # need to do a lot more complicated stuff if there is a star in the phrase search
        expression = re.compile(r'\b'+ r' '.join([('(' + k +')').replace('*)',')[a-zA-Z]*') for k in phrase.split(' ')]) + r'\b')
        return bool(expression.search(string))
    
    # if there isn't a star, then we can do a simple moving window
    split = string.split()
    n = len(phrase.split()) # number of words in our moving window
    return any((phrase == ' '.join(split[i:(i+n)]) for i in range(len(split) - n + 1))) # build moving phrase window and compare
    
# phrase_search('i am hunkering down out here in alaska',"hunker* down")
# phrase_search('just got my stimulus checks its great','stimulus check*')

# near matching for: keyword1 near/n keyword2
def near_match(string,keyword1,keyword2,n):
    # quick and dirty way to check if any of the keywords are eligable
    if keyword1.replace('*','') not in string: return False
    if keyword2.replace('*','') not in string: return False
    
    # handle matching | return early to increase performance
    if keyword1.find('*') >=0:
        k1check = star_match
        keyword1 = keyword1.replace('*','')
        if not star_search(string, keyword1): return False # faster than checking everything (maybe)
    else:
        k1check = full_match
        #if not full_search(string,keyword1): return False
    if keyword2.find('*') >=0:
        k2check = star_match
        keyword2 = keyword2.replace('*','')
        if not star_search(string, keyword2): return False
    else:
        k2check = full_match   
        #if not full_search(string,keyword2): return False

    k1 = np.array([])
    k2 = np.array([])
    
    min_length = min([len(keyword1),len(keyword2)])
    
    for i,w in enumerate(string.split()):
        if len(w) < min_length: continue
        if k1check(w,keyword1):
            k1 = np.append(k1,i)
        elif k2check(w,keyword2):
            k2 = np.append(k2,i)
    # # compute distance matrix
    distance = np.abs(k1[:, np.newaxis] - k2)
    # # return True if any distance in the distance matrix is less than or equal to n
    return np.any(distance <= n)

# split query into multiple filters by building a list of partial functions 
# When using the list of filters, use any() to ensure lazy evaluation
def build_filters(filter_string):
    filt = filter_string.lower().replace('"','').split(' or ') # remove quotations and split on different conditions
    filt_list = []
    # go through each filter and build a PARTIAL function for the corresponding logic to be used for filtering
    for f in filt:
        f = f.strip() # remove unwanted spaces 
        if "near/" in f: # near follows the format of: "(keyword1 near/n keyword2)"
            f0 = f.replace('(','').replace(')','').split(' ') # remove parathensis
            n = int(f0[1][-1])
            keyword1 = f0[0]
            keyword2 = f0[-1]
            filt_list.append(partial(near_match,keyword1 = keyword1, keyword2 = keyword2, n=n))
        elif f.find('*') >= 0:
            if len(f.split()) == 1:
                f = f.replace('*','')
                filt_list.append(partial(star_search,keyword=f))
            else:
                filt_list.append(partial(phrase_search,phrase = f))
        elif len(f.split()) > 1:
            filt_list.append(partial(phrase_search,phrase=f))
        else:
            filt_list.append(partial(full_search,keyword=f))
    return filt_list

#%%

# set preprocess = True if you havent fitlered city, scrubbed_text or  
def do_filter(df, names = names, preprocess = False, city_filt = True):
    j = 0 # lets be safe here 
    def funky(string):
        # same thing as lazy evaluation
        for p in build_filters(queries[j]):
            if p(string):
                return 1 
        return 0
    
    if preprocess:
        global region
        if city_filt:
            df = filter_city(df,region).reset_index(drop=True)
        df['scrubbed_text'] = df['full_text'].apply(prepare_text)
    for i in range(len(names)):
        j = i # not sure if I should be able to refer to this i in the funky() function, so I'm being safe
        df[names[i]] = df.scrubbed_text.apply(funky)
    return df

def preprocess_corpus(df,region):
    df = filter_city(df,region).reset_index(drop=True)
    df['scrubbed_text'] = df['full_text'].apply(prepare_text)
    #df = df.drop(['full_text','tweet_date','user_location','seconds'],axis=1)
    return df

