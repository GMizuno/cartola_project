from pandas import DataFrame


def win_home(data: DataFrame):
    if data.goals_home == data.goals_away:
        return 'home_draw'
    elif data.goals_home > data.goals_away:
        return 'home_win'
    else:
        return 'home_lose'


def win(data: DataFrame):
    if data.win_home == 'home_draw':
        return 'draw'
    elif data.win_home == 'home_win' and data.home == True:
        return 'win'
    else:
        return 'lose'
