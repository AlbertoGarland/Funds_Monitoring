from datetime import datetime
import holidays
import pandas as pd
import time
import sys

def valide_date(date: datetime):
    year = int(date.year)
    fr_holidays = holidays.France(years=year)
    index_date = date.weekday()

    # Vérification si la date est un week-end ou un jour férié
    while index_date >= 5 or date in fr_holidays:
        if index_date == 6:  # Dimanche
            day_spread = 2
        else:  # Samedi ou jour férié pendant la semaine
            day_spread = 1
        # Ajustement de la date
        date = date - pd.Timedelta(days=day_spread)
        index_date = date.weekday()

    return date

def minuteur(duree):
    while duree:
        minutes,seconds = divmod(duree,60)
        sys.stdout.write(f"\r{minutes:02d}:{seconds:02d}")
        sys.stdout.flush()
        time.sleep(1)
        duree -= 1
    sys.stdout.write("\r00:00")
    sys.stdout.flush()
    print('\rTemps écoulé!')
if __name__ == '__main__':
    minuteur(5)
