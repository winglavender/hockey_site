
import pandas as pd

class SeasonCalculator:

    def __init__(self, today, config):
        self.seasons = []
        self.season_dates = {}
        self.year_to_season = {}
        self.config = config
        self.today = today
        self.read_season_dates()  # all use cases require season dates

    def get_season_dates(self, season):
        return self.season_dates[season]

    def get_latest_season(self):
        # return the last season to be considered for scraping
        # if we are in the offseason, the latest season is NEXT season (because next season's rosters are being set by trades now)
        # if we are in the season, the latest season is THIS season
        season_name, in_season = self.get_season_from_date(self.today)
        if in_season:
            return season_name
        else:
            return self.get_next_season(season_name)

    def read_season_dates(self):
        # read from file of season dates: name, start date, end date
        season_dates = []
        with open(self.config['root_dir'] + '/hockey_db_data/nhl_season_dates.txt') as in_file:
            for line in in_file:
                season_dates.append(line.strip().split(","))
        # iterate backwards over season dates to define season structure
        season_dates.reverse()
        # fake next season start
        end_year = season_dates[0][2].split()[2]
        next_start = pd.to_datetime(f"10/1/{end_year}")
        for season_tmp in season_dates:
            name = season_tmp[0]
            self.seasons.append(name)
            # get start, end, end offseason dates
            start_date = pd.to_datetime(season_tmp[1])
            end_season_date = pd.to_datetime(season_tmp[2])
            end_offseason_date = next_start - pd.to_timedelta(1, unit='d')
            self.season_dates[name] = (start_date, end_season_date, end_offseason_date)
            next_start = start_date
        # parse again to get year_to_season info (we need next season's start date to properly do this because of weird offseasons)
        for season_tmp in season_dates:
            name = season_tmp[0]
            start_date, end_season_date, end_offseason_date = self.season_dates[name]
            for year in range(start_date.year, end_offseason_date.year+1):
                if year not in self.year_to_season:
                    self.year_to_season[year] = []
                self.year_to_season[year].append(name)
        self.seasons.reverse() # we want this list in forwards order

    def get_next_season(self, season_name):
        idx = self.seasons.index(season_name)
        return self.seasons[idx+1]

    def get_prev_season(self, season_name):
        idx = self.seasons.index(season_name)
        return self.seasons[idx-1]

    def season_to_years(self, season_name):
        return season_name.split("-")

    def date_is_in_season_span(self, start_season, end_season, date):
        if self.get_season_dates(start_season)[0] <= date <= self.get_season_dates(end_season)[2]:
            return True
        return False

    def date_is_in_season(self, season, date):
        start_date, end_date, off_end_date = self.get_season_dates(season)
        if date >= start_date and date <= off_end_date: # date occurs in this season
            # check if in season or off season
            if date <= end_date:
                return True, True # date is in season (including offseason) and is during the actual season
            else:
                return True, False # date is in season but occurred during the following offseason
        return False, False

    def get_season_from_date(self, date):
        # returns season name (aka 2021-2022) and whether the date was in season True/False
        # (if False, date occurs in off season FOLLOWING the named season)
        potential_seasons = self.year_to_season[date.year]
        for season in potential_seasons:
            curr_season_dates = self.season_dates[season]
            if date >= curr_season_dates[0] and date <= curr_season_dates[2]: # date happened during this season (including offseason)
                if date <= curr_season_dates[1]: # date happened in season proper
                    return season, True
                else: # date happened in offseason
                    return season, False
        # didn't match to any potential season
        print("GET SEASON FROM DATE ERROR")
        print(date)
        print(potential_seasons)
        return "", False

    def adjacent_seasons(self, season1, season2):
        # assumes season1 comes before season2
        years1 = self.season_to_years(season1)
        years2 = self.season_to_years(season2)
        if years1[1] == years2[0]:
            return True
        else:
            return False
