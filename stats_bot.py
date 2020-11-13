#####################################
'''
Covid Statistics Twitter Bot
Sam Wallach Hanson

This bot synthesizes data from NYT and
the Covid Tracking Project, tweeting out
key stats. If you're reading this, please
feel free to reach out to sam.wallach.hanson@gmail.com
with metrics you'd like to see included.
'''
#####################################


import tweepy, keys, git, time, pandas as pd, numpy as np, sys, os, requests
from datetime import date, timedelta

# Twitter API keys from keys file (not on Github), returns twitter bot instance
def startup():

    auth = tweepy.OAuthHandler(keys.authkey,keys.authkeysecret)
    auth.set_access_token(keys.accesskey,keys.accesskeysecret)

    api = tweepy.API(auth)

    return api

# Pulls latest NYT data to local repo
def update_nyt(pathname):
    git_dir = os.path.join(pathname, 'covid-19-data')
    g = git.cmd.Git(git_dir)
    g.pull()

# Returns Pandas df of case and death data by state, from NYT
def import_nyt(pathname):
    #NYT States data
    states_df = pd.read_csv(os.path.join(pathname, 'covid-19-data/us-states.csv'))
    states_df = states_df.sort_values(by=['state', 'date'])
    #Daily state cases and deaths
    states_df['daily_cases'] = states_df.groupby(['state'])['cases'].diff().fillna(states_df['cases'])
    states_df['daily_deaths'] = states_df.groupby(['state'])['deaths'].diff().fillna(states_df['deaths'])

    return states_df

# Returns Pandas df of census population data by state, for case density metrics
def import_census(pathname):
    #Census Population Data
    population_df = pd.read_csv(os.path.join(pathname, 'main_project/data/census_population.csv'))

    population_states = population_df[population_df['COUNTY']==0]
    population_states = population_states.copy()[['STATE', 'STNAME', 'POPESTIMATE2019']]
    population_states.columns = ['fips', 'state', 'population']

    return population_states

# Returns df of state level data from covid traacking project
def import_covid_tracker(pathname):
    #Covid Tracker
    url = 'https://covidtracking.com/api/v1/states/daily.csv'

    req = requests.get(url)
    url_content = req.content

    #Write downloaded data to file
    with open(os.path.join(pathname, 'main_project/data/covid_tracker_states.csv'), 'wb') as f:
        f.write(url_content)

    covid_tracker_df = pd.read_csv(os.path.join('main_project/data/covid_tracker_states.csv'))

    covid_tracker_df['date'] = covid_tracker_df['date'].astype(str)
    covid_tracker_df['date'] = covid_tracker_df['date'].str[:4] + "-" + covid_tracker_df['date'].str[4:6] + "-" + covid_tracker_df['date'].str[6:]

    return covid_tracker_df

# Helper function for case and death string formatting
# Returns a string of either nothing, a single state, 'state AND state', or
# 'state, state, ..., AND state'
def format_max_tweet(case_death_list):
    if len(case_death_list) == 0:
        return ""
    elif len(case_death_list) == 1:
        return case_death_list[0]
    elif len(case_death_list) == 2:
        return case_death_list[0] + " and " + case_death_list[1]
    else:
        return ', '.join(case_death_list[:len(case_death_list)-1]) + " and " + case_death_list[len(case_death_list)-1]

#Metric 1: States with the most daily cases or deaths they have seen up until this point
def m1_daily_maxes(states_df, formatted_timestr):

    # State(s) who saw max cases yesterday
    max_cases = states_df.groupby(['state'])['daily_cases'].transform(max) == states_df['daily_cases']
    max_cases = states_df[max_cases]
    max_cases = max_cases[max_cases['date']==formatted_timestr]

    max_deaths = states_df.groupby(['state'])['daily_deaths'].transform(max) == states_df['daily_deaths']
    max_deaths = states_df[max_deaths]
    max_deaths = max_deaths[max_deaths['date']==formatted_timestr]

    case_list = max_cases['state'].tolist()
    death_list = max_deaths['state'].tolist()

    cases_str = format_max_tweet(case_list)
    deaths_str = format_max_tweet(death_list)


    # String formatting
    output_str = ""
    if cases_str != "":
        if len(case_list) == 1:
            output_str = "The state that beat its previous case maximum was %s" % cases_str
        else:
            output_str = "The states that beat their previous case maximums were %s" % cases_str
        if deaths_str != "":
            if len(death_list) == 1:
                output_str = output_str + ", and the state that beat its previous death maximum was %s" % deaths_str
            else:
                output_str = output_str + ", and the states that beat their previous death maximums were %s" % deaths_str
        output_str = output_str + "."
    elif deaths_str != "":
        if len(death_list) == 1:
            output_str =  "The state that beat its previous death maximum was %s" % deaths_str
        else:
            output_str =  "The states that beat their previous death maximums were %s" % deaths_str

    if len(max_cases.index) == 1:
        num_str_case = "Yesterday, 1 state saw its highest daily case total"
    else:
        num_str_case = "Yesterday, %i states saw their highest daily case totals" % len(max_cases.index)

    if len(max_deaths.index) == 1:
        num_str_deaths = "and 1 state saw its highest daily deaths"
    else:
        num_str_deaths = "and %i states saw their highest daily deaths" % len(max_deaths.index)

    return_str = "%s, %s. %s" % (num_str_case, num_str_deaths, output_str)
    return return_str

#Metric 2: States with the highest cases and death/per capita yesterday/in the last week
def m2_highest_yesterday(states_df, population_states, formatted_timestr):


    yesterday_df = states_df[states_df['date']==formatted_timestr]
    yesterday_df = pd.merge(yesterday_df, population_states,  how='inner', left_on=['fips','state'], right_on = ['fips','state'])

    yesterday_df['cases_per_100K_daily'] = (yesterday_df['daily_cases']/yesterday_df['population'])*100000
    yesterday_df['deaths_per_100K_daily'] = (yesterday_df['daily_deaths']/yesterday_df['population'])*100000

    max_cases_yesterday_population_adjusted = yesterday_df[yesterday_df.cases_per_100K_daily == yesterday_df.cases_per_100K_daily.max()].reset_index(drop=True)
    max_deaths_yesterday_population_adjusted = yesterday_df[yesterday_df.deaths_per_100K_daily == yesterday_df.deaths_per_100K_daily.max()].reset_index(drop=True)
    max_cases_yesterday = yesterday_df[yesterday_df.daily_cases == yesterday_df.daily_cases.max()].reset_index(drop=True)
    max_deaths_yesterday = yesterday_df[yesterday_df.daily_deaths == yesterday_df.daily_deaths.max()].reset_index(drop=True)


    return_str_1 = "The state with the most cases yesterday was %s with %i. Adjusting for population size, the state with the most cases yesterday was %s, with %.2f per 100K people." % (max_cases_yesterday['state'][0], max_cases_yesterday['daily_cases'][0], max_cases_yesterday_population_adjusted['state'][0], max_cases_yesterday_population_adjusted['cases_per_100K_daily'][0])
    return_str_2 = "For deaths, the state with the most yesterday was %s with %i. Adjusting for population size, the state with the most deaths yesterday was %s, with %.3f per 100K people." % (max_deaths_yesterday['state'][0], max_deaths_yesterday['daily_deaths'][0], max_deaths_yesterday_population_adjusted['state'][0], max_deaths_yesterday_population_adjusted['deaths_per_100K_daily'][0])

    return (return_str_1, return_str_2)


#Metric 3: State with the highest positive test rate
def m3_oneday_positivity_rate(covid_tracker_df, formatted_timestr):

    yesterday_covid_tracker_df = covid_tracker_df.copy()[covid_tracker_df['date']==formatted_timestr]
    yesterday_covid_tracker_df['positive_rate'] = yesterday_covid_tracker_df['positiveIncrease']/yesterday_covid_tracker_df['totalTestResultsIncrease']
    yesterday_covid_tracker_df.loc[yesterday_covid_tracker_df['positive_rate'] >= 1, 'positive_rate' ] = 0

    max_rate_yesterday = yesterday_covid_tracker_df[yesterday_covid_tracker_df.positive_rate == yesterday_covid_tracker_df.positive_rate.max()].reset_index(drop=True)
    max_rate_yesterday['positive_rate'] = max_rate_yesterday['positive_rate'] * 100

    return_str = "The state with the highest 1-day positivity rate was %s, with %.2f%% of all tests coming back positive." % (max_rate_yesterday['state'][0], max_rate_yesterday['positive_rate'][0])
    return return_str

# Metric 4: Highest Positive 7-day test rate
def m4_sevenday_positivity_rate(covid_tracker_df, formatted_timestr):
    # Get current day
    today = date.today()
    yesterday = today - timedelta(1)

    week_list = []
    day = date.today()
    for i in range(7):
        day = day - timedelta(1)
        week_list.append(str(day))

    sevenday_covid_tracker_df = covid_tracker_df.copy()[covid_tracker_df['date'].isin(week_list)]
    sevenday_covid_tracker_df.loc[sevenday_covid_tracker_df['negativeIncrease'] == 0, 'positiveIncrease' ] = 0
    sevenday_covid_tracker_df.loc[sevenday_covid_tracker_df['negativeIncrease'] == 0, 'totalTestResultsIncrease' ] = 0

    # collapse on sum
    sevenday_covid_tracker_df = sevenday_covid_tracker_df.groupby(['state']).sum()

    sevenday_covid_tracker_df['positive_rate'] = sevenday_covid_tracker_df['positiveIncrease']/sevenday_covid_tracker_df['totalTestResultsIncrease']
    sevenday_covid_tracker_df.loc[sevenday_covid_tracker_df['positive_rate'] >= 1, 'positive_rate' ] = 0
    max_rate_7day = sevenday_covid_tracker_df.copy()[sevenday_covid_tracker_df.positive_rate == sevenday_covid_tracker_df.positive_rate.max()]
    max_rate_7day['positive_rate'] = max_rate_7day['positive_rate'] * 100

    return_str = "The state with the highest 7-day positivity rate was %s, with %.2f%% of all tests coming back positive." % (max_rate_7day.index.tolist()[0], max_rate_7day['positive_rate'][0])
    return return_str

#Tweet Shortener, keeping tweets under character limit
def shortener(input_str, tbot, base_tweet):

    last_tweet_index = len(input_str) / 280

    for i in range((len(input_str) / 280)+1):
        if i == last_tweet_index:
            to_send = input_str[i*280:]
        else:
            start = (i*280)
            end = (i+1)*280
            to_send = input_str[start:end]

        tbot.update_status(to_send, base_tweet.id)


# Run everything, send the tweets
def main():

    os.chdir('..')
    cwd = os.getcwd()

    update_nyt(cwd)

    states_df = import_nyt(cwd)
    population_states = import_census(cwd)
    covid_tracker_df = import_covid_tracker(cwd)

    # Get current day
    today = date.today()
    yesterday = today - timedelta(1)
    formatted_timestr = str(yesterday)

    first = "This is the Covid-19 stats bot report for %s." % formatted_timestr

    # Generate tweets
    daily_max_str = m1_daily_maxes(states_df, formatted_timestr)
    highest_cases_yesterday_str = m2_highest_yesterday(states_df, population_states, formatted_timestr)[0]
    highest_deaths_yesterday_str = m2_highest_yesterday(states_df, population_states, formatted_timestr)[1]
    one_day_positivity = m3_oneday_positivity_rate(covid_tracker_df, formatted_timestr)
    seven_day_positivity = m4_sevenday_positivity_rate(covid_tracker_df, formatted_timestr)



    # Tweet the tweets!
    if sys.argv[1] == "send":
        api = startup()
        api.update_status(first)

        time.sleep(20)
        tweet = api.user_timeline(id = api.me().id, count = 1)[0]

        shortener(daily_max_str, api, tweet)
        shortener(highest_cases_yesterday_str, api, tweet)
        shortener(highest_deaths_yesterday_str, api, tweet)
        shortener(one_day_positivity, api, tweet)
        shortener(seven_day_positivity, api, tweet)


    # Or test the tweets
    else:
        print(first)
        print(daily_max_str)
        print(highest_cases_yesterday_str)
        print(highest_deaths_yesterday_str)
        print(one_day_positivity)
        print(seven_day_positivity)



if __name__ == "__main__":
    main()
