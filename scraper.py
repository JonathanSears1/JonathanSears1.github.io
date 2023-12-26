import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from random import randint
import re
import io
import numpy as np

master_df_ml = pd.read_csv('Data/final_master_for_ml.csv',dtype={'date':object,'season':'float','neutral':object,'playoff':object,'home_team':object,'away_team':object,'qb_away':object,'qb_home':object})
master_df = pd.read_csv('Data/final_master.csv',dtype={'date':object,'season':'float','neutral':object,'playoff':object,'home_team':object,'away_team':object,'qb_away':object,'qb_home':object})
teams = pd.read_csv('./Data/nfl_teams.csv')
past_scores = pd.read_csv('./Data/box_scores_cleaned.csv')
def get_box_score_link(year):
    user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    ]
    links = []
    header = {"User-Agent": user_agents[randint(0,len(user_agents) - 1)]}
    r = requests.get("https://www.footballdb.com/games/index.html",params={"lg":"NFL","yr":year},headers=header)
    soup = BeautifulSoup(r.content,"html.parser")
    tables = soup.find_all("table",class_ = "statistics")
    tables

    for table in tables:
        tbl_links = (table.find_all("a", href = True))
        for link in tbl_links:
            links.append("https://www.footballdb.com/"+link['href'])
    return links

def scrape_box_score(url):
    #links to all the box scores on footballDB
    #parse out the date and matchup info from the URL
    user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    ]
    matchup_date = url.split('/')[-1][:-3]
    print(matchup_date)
    date = matchup_date.split('-')[-1]
    print(date)
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    if len(month) != 2:
        month = "0" + month
    if len(day) != 2:
        day = "0" + day
    date = year + '-' + month + '-' + day
    print(date)
    matchup = " ".join([str(item) for item in matchup_date.split('-')[:-1]])
    #request and parse data into a DF using parse_req()
    header = {"User-Agent": user_agents[randint(0,len(user_agents) - 1)]}
    r = requests.get(url,headers=header)
    box_score = parse_req(r,date,matchup)
    return box_score

def parse_req(r,date,matchup):
    #parse the request to get the the box score tables
    soup = BeautifulSoup(r.content,'html.parser')
    stats = soup.find('div',id='divBox_team')
    table = stats.find_all('table',class_ = 'statistics')
    table_str = io.StringIO(str(table))
    tables = pd.read_html(table_str)
    pre_box_score = pd.concat(tables)
    #pre_box_score['gamedID'] = gameid
    pre_box_score.set_index('Unnamed: 0',inplace=True)
    box_score = pre_box_score.T
    box_score.reset_index(inplace=True)
    box_score.rename_axis(None, axis=1,inplace=True)
    box_score['date'] = date
    box_score['matchup'] = matchup
    if "Time of Possesion" not in box_score.columns:
        box_score['Time of Possession'] = np.nan
    if "Fourth Downs" not in box_score.columns:
        box_score["Fourth Downs"] = np.nan
    if len(box_score.columns) != len(list(set(box_score.columns))):
        seen = set()
        dupes = []

        for x in box_score.columns:
            if x in seen:
                dupes.append(x)
            else:
                seen.add(x)
        new_cols = []
        count = 1
        for col in box_score.columns:
            if col in dupes:
                new_cols.append(f"{col}_{count}")
                count += 1
            else:
                new_cols.append(col)
        box_score.columns = new_cols    
    return box_score

def scrape_scores(urls):
    box_scores = scrape_box_score(urls[0])
    for url in urls[1:]:
        box_score = scrape_box_score(url)
        box_scores = pd.concat([box_scores,box_score],join='outer',axis=0)
        box_scores.reset_index(inplace=True,drop=True)
    return box_scores

def clean_box_scores(box_scores_df):
    columns = {'Att - Comp - Int':['passing-attempts','completions','int-thrown'],
           'Interception Returns':['interceptions', 'int-return-yards'],
           'Fumbles - Lost':['fumbles','fumbles-lost'],
           'Field Goals': ['fga','fgm'],
           'Third Downs': ['3rd-down-convs','3rd-downs','3rd-down-conv-rate'],
           'Punts - Average': ['punts','yards-per-punt'],
           'Penalties - Yards':['penalties','penalty-yards'],
           'Sacked - Yds Lost':['sacks_allowed','sack-yds-lost'],
           'Punt Returns':['punts-returned','punt-return-yds'],
           'Kickoff Returns':['kicks-returned','kick-return-yds']
            }
    #First rename some columns 
    box_scores_df.rename(columns={"index":"team",
                              "First downs":"total-first-downs",
                              "Rushing": "rushing-first-downs",
                              "Passing": "passing-first-downs",
                              "Penalty": "penalty-first-downs",
                              "Average Gain_1": "avg-gain-rushing",
                              "Avg. Yards/Att": "yards-per-att",
                              "Rushing Plays": "rushing-plays",
                              "Total Net Yards": "net-yards",
                              "Net Yards Rushing": "net-rushing-yds",
                              "Net Yards Passing": "net-passing-yds",
                              "Gross Yards Passing":"gross-passing-yds",
                              "Avg. Yds/Att": "yds-per-att",
                              "Had Blocked": "blocked-kicks-allowed",
                              "Time of Possesion": "time-of-possession",
                              "Total Plays": "total-plays",
                              "Average Gain_2": "avg-gain-per-play",
                              },inplace=True)
    box_scores_df.replace({"--":"-", "":"0"},regex=True, inplace=True)
    for col in columns.keys():
        box_scores_df[columns[col]] = box_scores_df[col].str.split('-',expand=True)
    box_scores_df.drop(columns.keys(),axis =1, inplace=True)
    box_scores_df[['4th-down-convs','4th-downs','4th-down-conv-rate']] = box_scores_df['Fourth Downs'].str.split('-',expand=True)
    box_scores_df[box_scores_df == ''] = np.NaN
    box_scores_df.drop('Fourth Downs', axis=1,inplace=True)
    box_scores_df['team-abrev'] = box_scores_df['team'].str[-3:]
    def check_team_abrev(df):
        id = df['team-abrev']
        id = id.upper()
        if id in list(teams['team_id']):
            return id
        else:
            return id[1:]
    box_scores_df['team-abrev'] = box_scores_df.apply(check_team_abrev,axis=1)
    box_scores_df['team-abrev'].replace({"AK":"LVR"},inplace=True)
    return box_scores_df

def get_updated_scores(new_urls):
    scores = scrape_scores(new_urls)
    cleaned_scores = clean_box_scores(scores)
    return cleaned_scores

def main():
    past_scores['date']
    new_links = get_box_score_link(2023)
    new_scores = scrape_scores(new_links)
    cleaned_scores = clean_box_scores(new_scores)
    scores = pd.concat([past_scores,cleaned_scores],axis=0,join='outer')
    clean_scores_csv = scores.to_csv()
    with open("Data/box_scores_cleaned.csv", "w") as fp:
        fp.write(clean_scores_csv)
main()
