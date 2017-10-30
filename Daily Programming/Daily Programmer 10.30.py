
daydic = {2:"Sunday", 3:"Monday", 4:"Tuesday", 5:"Wednesday", 6:"Thurdsay", 0:"Friday", 1:"Saturday"}
monthdic = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30,]


year = int(input())
month = int(input())
day = int(input())

daysdays_raw = int(day)

def yearcount(year_fun):
    yeardays = 0
    for i in range(int(year_fun) - 1):
        if i % 4 != 0:
            yeardays += 365
        elif i % 100 != 0:
            yeardays += 366
        elif i % 400 != 0:
            yeardays += 365
        else:
            yeardays += 366
    return yeardays


def monthcount(month_fun, year_fun):
    monthdays = 0
    if int(month_fun) < 3:
        for i in range(month_fun):
            monthdays += monthdic[i]
        return monthdays
    elif year_fun % 4 != 0:
        for i in range(month_fun):
            monthdays += monthdic[i]
        return monthdays
    elif year_fun % 100 != 0:
        for i in range(month_fun):
            monthdays += monthdic[i]
        monthdays += 1
        return monthdays
    elif year_fun % 400 != 0:
        for i in range(month_fun):
            monthdays += monthdic[i]
        return monthdays
    else:
        for i in range(month_fun):
            monthdays += monthdic[i]
        monthdays += 1
        return monthdays


totaldays = yearcount(year) + daysdays_raw + monthcount(month, year)

# Epoch date is 0000, 12, 31

daynum = totaldays % 7

print(daydic[daynum])

