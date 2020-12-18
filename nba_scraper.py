import os
import time
import pandas as pd

from nba_api.stats.endpoints.commonallplayers import CommonAllPlayers
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.shotchartdetail import ShotChartDetail
from nba_api.stats.endpoints.playbyplayv2 import PlayByPlayV2
from nba_api.stats.endpoints.boxscoreadvancedv2 import BoxScoreAdvancedV2

def calculate_time_at_period(period):
    if period > 5:
        return (720 * 4 + (period - 5) * (5 * 60)) * 10
    else:
        return (720 * (period - 1)) * 10

# util func that takes a year like '2019' and returns '2019-20'
def year_str(year):
    next_year = int(year) + 1
    next_year_str = str(next_year)[2:4]
    return '{}-{}'.format(year, next_year_str)

# util func that gets all the game_ids for a given year
# requires that we already have downloaded all the players
# and their game logs b/c we parse the game_ids from there
def game_ids_for(year):
    all_game_ids = set()
    players = pd.read_csv('./{}/players.csv'.format(year))
    player_ids = players['PERSON_ID'].to_list()
    for player_id in player_ids:
        # game_ids start with leading 0s and panda strips this if we don't tell it explicitly not to by
        # specifiying that the column type is a string
        player_game_log = pd.read_csv('./{}/player_game_logs/{}.csv'.format(year, player_id), dtype={'Game_ID':str})
        game_ids = player_game_log['Game_ID'].to_list()
        # make sure all the games this player played in get added to the  set
        all_game_ids.update(game_ids)
    return list(all_game_ids)

# sticks all players that played in a specific year and writes to a file
def download_players(year):
    dir = './{}'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    file_name = './{}/players.csv'.format(year)
    # no need to re-fetch what we already have
    if (os.path.exists(file_name)):
        return
    players = CommonAllPlayers().get_data_frames()[0]
    active = players[(players['FROM_YEAR'].astype(int) <= int(year)) & (players['TO_YEAR'].astype(int) >= int(year))]
    active.to_csv(file_name)

def download_game_logs(year, player_id):
    file_name = './{}/player_game_logs/{}.csv'.format(year, player_id)
    # no need to re-fetch what we already have
    if (os.path.exists(file_name)):
        return
    # prevent throttling by sleeping 3 sec before each request
    time.sleep(3)
    game_logs = PlayerGameLog(player_id=player_id, season=year_str(year)).get_data_frames()[0]
    game_logs.to_csv(file_name)

def download_game_logs_for(year):
    dir = './{}/player_game_logs'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)

    players = pd.read_csv('./{}/players.csv'.format(year))
    ids = players['PERSON_ID'].to_list()
    i = 0
    while (i < len(ids)):
        try:
            print('Downloading {} game logs for person_id:{}'.format(year, ids[i]))
            download_game_logs(year, ids[i])
        except:
            # on error set index back one, sleep for a few min, and get back to it!
            print('Failed downloading {} game logs for person_id:{}'.format(year, ids[i]))
            i = i -1
            time.sleep(300)
        i = i + 1

def download_shot_chart_details(year, player_id):
    file_name = './{}/player_shot_chart_details/{}.csv'.format(year, player_id)
    if (os.path.exists(file_name)):
        return
    # prevent throttling by sleeping 3 sec before each request
    time.sleep(3)
    # passing 0 for team and player will give us all entries, context_measure_simple 
    shot_chart_detail = ShotChartDetail(team_id='0', player_id=player_id, season_nullable=year_str(year), context_measure_simple='FGA').get_data_frames()[0]
    shot_chart_detail.to_csv(file_name)

def download_shot_chart_details_for(year):
    dir = './{}/player_shot_chart_details'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)

    players = pd.read_csv('./{}/players.csv'.format(year))
    ids = players['PERSON_ID'].to_list()
    i = 0
    while (i < len(ids)):
        try:
            print('Downloading {} shot chart details for person_id:{}'.format(year, ids[i]))
            download_shot_chart_details(year, ids[i])
        except:
            # on error set index back one, sleep for a few min, and get back to it!
            print('Failed downloading {} shot chart details for person_id:{}'.format(year, ids[i]))
            i = i -1
            time.sleep(300)
        i = i + 1

def download_play_by_play(year, game_id):
    file_name = './{}/game_play_by_play/{}.csv'.format(year, game_id)
    if (os.path.exists(file_name)):
        return
    # prevent throttling by sleeping 3 sec before each request
    time.sleep(3)
    play_by_play = PlayByPlayV2(game_id=game_id).get_data_frames()[0]
    play_by_play.to_csv(file_name)

def download_play_by_play_for(year):
    dir = './{}/game_play_by_play'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)

    ids = game_ids_for(year)
    i = 0
    while (i < len(ids)):
        try:
            print('Downloading {} play by play for game_id:{}'.format(year, ids[i]))
            download_play_by_play(year, ids[i])
        except:
            # on error set index back one, sleep for a few min, and get back to it!
            print('Failed downloading {} play by play for game_id:{}'.format(year, ids[i]))
            i = i -1
            time.sleep(300)
        i = i + 1

def download_boxscore_advanced(year, game_id):
    file_name = './{}/game_advanced_boxscore/{}.csv'.format(year, game_id)
    if (os.path.exists(file_name)):
        return
    # prevent throttling by sleeping 3 sec before each request
    time.sleep(3)
    boxscore_advanced = BoxScoreAdvancedV2(game_id=game_id).get_data_frames()[0]
    boxscore_advanced.to_csv(file_name)

def download_boxscore_advanced_for(year):
    dir = './{}/game_advanced_boxscore'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)

    ids = game_ids_for(year)
    i = 0
    while (i < len(ids)):
        try:
            print('Downloading {} boxscore advanced for game_id:{}'.format(year, ids[i]))
            download_boxscore_advanced(year, ids[i])
        except:
            # on error set index back one, sleep for a few min, and get back to it!
            print('Failed downloading {} boxscore advanced for game_id:{}'.format(year, ids[i]))
            i = i -1
            time.sleep(300)
        i = i + 1

def download_boxscore_advanced_by_quarter(year, game_id):
    play_by_play = pd.read_csv('./{}/game_play_by_play/{}.csv'.format(year, game_id))
    periods = list(set(play_by_play['PERIOD'].to_list()))
    for period in periods:
        file_name = './{}/game_advanced_boxscore_by_quarter/{}_{}.csv'.format(year, game_id, period)
        if (os.path.exists(file_name)):
            continue
        # prevent throttling by sleeping 3 sec before each request
        time.sleep(3)
        start = calculate_time_at_period(period) + 5
        end = calculate_time_at_period(period + 1) - 5
        boxscore_advanced = BoxScoreAdvancedV2(game_id=game_id, start_range=start, end_range=end, start_period=0, end_period=14, range_type=2).get_data_frames()[0]
        boxscore_advanced.to_csv(file_name)

def download_boxscore_advanced_by_quarter_for(year):
    dir = './{}/game_advanced_boxscore_by_quarter'.format(year)
    if not os.path.exists(dir):
        os.makedirs(dir)

    ids = game_ids_for(year)
    i = 0
    while (i < len(ids)):
        try:
            print('Downloading {} boxscore advanced by quarter for game_id:{}'.format(year, ids[i]))
            download_boxscore_advanced_by_quarter(year, ids[i])
        except:
            # on error set index back one, sleep for a few min, and get back to it!
            print('Failed downloading {} boxscore advanced by quarter for game_id:{}'.format(year, ids[i]))
            i = i -1
            time.sleep(300)
        i = i + 1

download_players('2019')
download_game_logs_for('2019')
download_shot_chart_details_for('2019')
# before executing these two, which rely on game_ids, we need to get all players and game_logs
download_play_by_play_for('2019')
download_boxscore_advanced_for('2019') 
# this is actually the same endpoint as the previous but broken down by period for each game
# b/c the previous statement downloads the entire game box score
download_boxscore_advanced_by_quarter_for('2019')
