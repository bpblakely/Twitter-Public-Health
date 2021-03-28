# Twitter-Public-Health
Project for Bowling Green State Universities Hackathon

# Project Idea

With the Hackathon's theme being public health, our project decides to look at the general public opinion of certain subjects relating to public health & covid by analyzing Tweets from Twitter. 

The subjects we consider are the following:
* **Covid**
* The **General Flu**
* **Masks**
* **Sanitation**
* **Social Distancing**
* Flu/Covid **Symptoms**
* **Covid Testing**
* Covid Treatments/**Vaccines**
* **Remote Working**.

If we have time, we will test to see if there is any correlation between a cities covid case counts and that cities general sentiment around covid.  

# Cities Analyzed

* Atlanta, Georgia
* Baltimore, Maryland
* Columbus, Ohio
* Chicago, Illinois
* Phoenix, Arizona
* Pittsburgh, Pennsylvania

# Data 

For each city we collected tweets from September 22nd 2020 to March 20th 2021, yielding a time series dataset spanning exactly 180 days.

Tweets are filtered by keywords related to each subject.

**Number of Tweets Analyzed: 171,607,760** 

Data Matrix: (171,607,760 x 24)

**City Data Breakdown**

| City       | Number of Tweets        | Covid  | Flu    | Masks  | Testing | Treatments | Sanitizing | Social Distancing | Symptoms | Working |
|------------|----------|--------|--------|--------|---------|------------|------------|-------------------|----------|---------|
| Atlanta    | 46270298 | 247777 | 132809 | 104868 | 2717    | 89060      | 13421      | 37080             | 16079    | 36182   |
| Baltimore  | 15027860 | 113708 | 51470  | 34377  | 1348    | 44168      | 4438       | 12905             | 6043     | 13957   |
| Chicago    | 50961706 | 427358 | 241476 | 173443 | 5025    | 167405     | 20035      | 65520             | 24800    | 67424   |
| Columbus   | 12488047 | 78640  | 39008  | 30904  | 870     | 27396      | 2864       | 10320             | 3958     | 8898    |
| Phoenix    | 15546687 | 155888 | 77111  | 71919  | 1521    | 51626      | 6235       | 22260             | 7601     | 18183   |
| Pittsburgh | 31313162 | 103241 | 54042  | 42238  | 1253    | 38796      | 3949       | 14473             | 5482     | 12014   |

*Note: Subjects are filtered by using multiple keywords. For example, the vaccines subject consists of keywords of multiple different covid vaccines (such as pfizer or moderna)*

# Tweet Data

We do not consider retweets in this dataset, thus every tweet we consider is unique. This helps us avoid duplicated data and you actually can't get geographic specificity on Twitter if you consider retweets. Keep in mind that a large majority of activity on Twitter is retweets.

A tweet consists of a timestamp, raw text/message, number of likes, number of retweets, user id, user's friend count, user's follower count, account age since creation, and a verified flag (1 means the user is verified)

