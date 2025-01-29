import datetime

if __name__ == '__main__':
    date = datetime.datetime.now().strftime("%Y%m%d_%H-%M-%S")
    print(date)