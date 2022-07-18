import json
import os
from matplotlib import pyplot as plt, ticker
import matplotlib.ticker as mticker
from datetime import datetime as dt
from alive_progress import alive_bar
import tools
import pandas as pd
import seaborn as sns


def line_plot(aggregate=False):
    data = crunchbase_users_parse(aggregate)
    dtt = pd.DataFrame(data)
    dtt = dtt[dtt['Year'] != 2021]
    dtt = dtt.groupby(by=["Year", "Nationality"])['Value'].sum().reset_index()
    dtt = dtt.sort_values(by=["Value", "Nationality"]).reset_index()
    dtt = dtt[dtt['Value'].notna()]
    dtt = (dtt.pivot_table(index='Nationality', columns='Year', values='Value', aggfunc="sum", fill_value=0,
                           margins=True).sort_values("All", ascending=False).drop("All", axis=0))
    for el in dtt.index:
        if dtt["All"][el] < 16:
            dtt = dtt.drop(el)
    dtt = dtt.drop("All", axis=1)
    dtt = dtt.head(10)
    dtt = dtt.reset_index().melt(id_vars=['Nationality'])
    dtt = pd.DataFrame(dtt.to_records())
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(16, 9))
    p = sns.lineplot(x="Year", y='value', ax=ax, hue="Nationality",
                     data=dtt, palette="tab10", linewidth=3)
    p.set_xlabel("Years", fontsize=20)
    p.set_ylabel("Values", fontsize=20)
    p.set_title("CrunchBase Users", fontsize=20)
    p.legend(fontsize=20)
    p.xaxis.set_major_locator(ticker.MultipleLocator(1))
    p.xaxis.set_major_formatter(ticker.ScalarFormatter())
    p.yaxis.set_major_locator(ticker.MultipleLocator(5000))
    p.yaxis.set_major_formatter(ticker.ScalarFormatter())
    p.set(yscale="log")
    p.set_xticklabels([str(int(i)) for i in p.get_xticks()], fontsize=20)
    p.set_yticklabels([str(int(i)) for i in p.get_yticks()], fontsize=20)
    plt.tight_layout()
    plt.savefig("CrunchLineplot.png")
    plt.clf()


def crunchbase_people_heatmap(aggregate=False):
    data = crunchbase_users_parse(aggregate)
    dtt = pd.DataFrame(data)
    dtt = dtt[dtt['Year'] != 2021]
    dtt = dtt.groupby(by=["Year", "Nationality"])['Value'].sum().reset_index()
    dtt = dtt.sort_values(by=["Value", "Nationality"]).reset_index()
    dtt = dtt[dtt['Value'].notna()]
    dtt = (dtt.pivot_table(index='Nationality', columns='Year', values='Value', aggfunc="sum", fill_value=0,
                           margins=True).sort_values("All", ascending=False).drop("All", axis=0))
    for el in dtt.index:
        if dtt["All"][el] < 500:
            dtt = dtt.drop(el)
    dtt = dtt.drop("All", axis=1)
    sns.set_theme(style="whitegrid")

    # plt.style.use("seaborn")
    plt.figure(figsize=(16, 9))
    p = sns.heatmap(dtt, annot=True, fmt="d", cmap="viridis", annot_kws={"fontsize": 16},
                    cbar_kws={'label': 'Users'})
    p.figure.axes[-1].yaxis.label.set_size(20)
    p.figure.axes[-1].yaxis.set_tick_params(labelsize=20)
    p.yaxis.set_tick_params(labelsize=15)
    p.xaxis.set_tick_params(labelsize=20)
    p.set_xlabel("Year", fontsize=20)
    p.set_ylabel("Nationality", fontsize=20)
    plt.tight_layout()
    plt.savefig("CrunchHeatMap.png")
    plt.clf()


def histograms(tp="Jobs"):
    count = tools.read_json("CrunchQR/" + tp + "_counter.json")
    df = pd.DataFrame(count)
    df = df[df["total"] > 0]
    df = df.rename_axis('index').reset_index()
    df = df[df["index"] < str(40)]

    sns.set_theme(style="white", context="talk")
    f, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 9), sharex=True)

    sns.barplot(x=df.index, y=df["male"], palette="icefire", ax=ax1)
    ax1.axhline(0, color="k", clip_on=False)
    ax1.set_ylabel("Male Users", fontsize=20)
    ax1.yaxis.set_major_locator(mticker.FixedLocator(ax1.get_yticks()))
    ax1.set_yticklabels([str(int(x)) for x in ax1.get_yticks()], fontsize=20)

    sns.barplot(x=df.index, y=df["female"], palette="icefire", ax=ax2)
    ax2.axhline(0, color="k", clip_on=False)
    ax2.set_ylabel("Female Users", fontsize=20)
    ax2.yaxis.set_major_locator(mticker.FixedLocator(ax2.get_yticks()))
    ax2.set_yticklabels([str(int(x)) for x in ax2.get_yticks()], fontsize=20)

    sns.barplot(x=df.index, y=df["total"], palette="icefire", ax=ax3)
    ax3.axhline(0, color="k", clip_on=False)
    ax3.set_ylabel("All Users", fontsize=20)
    ax3.set_xlabel("Number of " + tp, fontsize=20)
    ax3.xaxis.set_major_locator(mticker.FixedLocator(ax3.get_xticks()))
    ax3.set_xticklabels([str(int(x)) for x in ax3.get_xticks()], fontsize=20)
    # ax1.set_yscale("log")
    ax1.set_ylim(1)
    # ax2.set_yscale("log")
    ax2.set_ylim(1)
    ax3.set_yscale("log")
    ax3.set_ylim(1)

    plt.tight_layout()
    sns.despine(offset=3)
    plt.show()


def crunchbase_users_parse(aggregate=False, which='tot_users', jobs=False):
    if os.path.exists("CrunchBase Data\\users_per_year_" + str(aggregate) + ".json") and not jobs:
        with open("CrunchBase Data\\users_per_year_" + str(aggregate) + ".json", "r+") as use_file:
            return json.load(use_file)
    aggregate_countries_file = open("Countries\my_cont.json", "r+")
    aggregate_countries = json.load(aggregate_countries_file)
    my_db = tools.NestedDict()
    crunchbase_file = open("CrunchBase Data\\total3.json", "r+")
    crunchbase = json.load(crunchbase_file)

    with alive_bar(len(crunchbase), title="Users",
                   force_tty=True, spinner="classic") as users_bar:
        for person in sorted(crunchbase):
            users_bar()
            if not (account_creation_date := crunchbase[person]['person_account_creation']) or \
                    not (nationality := get_nationality(crunchbase[person]['degrees'],
                                                        crunchbase[person]['jobs'])):
                continue
            account_creation_date = dt.strptime(account_creation_date, "%Y-%m-%d %H:%M:%S")
            first_agg_off = tools.get_continent(nationality, aggregate_countries, aggregate)
            if which == 'tot_users':
                next_year = dt.strptime(str(2011), "%Y")
                if account_creation_date < next_year:
                    for year in sorted(range(2010, 2022)):
                        my_db[first_agg_off][year]['Value'] += 1
            for year in sorted(range(2011, 2022)):
                current_year = dt.strptime(str(year), "%Y")
                next_year = dt.strptime(str(year + 1), "%Y")
                if current_year < account_creation_date < next_year:
                    for remaining_year in range(year, 2022):
                        my_db[first_agg_off][remaining_year]['Value'] += 1

        for el in my_db:
            for year in sorted(range(2010, 2022)):
                if year not in my_db[el]:
                    my_db[el][year]['Value'] = 0
    new_db = []
    for el in my_db:
        for year in my_db[el]:
            new_db.append({"Nationality": el, "Year": year, "Value": my_db[el][year]['Value']})
    with open("CrunchBase Data\\users_per_year_" + str(aggregate) + ".json", "w+") as use_file:
        use_file.write(json.dumps(new_db, indent=4))
    return new_db


def get_nationality(person_degrees, person_jobs=None):
    location_counter = {}

    maxCountry = {'country': None, 'occurrences': 0, 'date': "2023/01/01"}

    for person_degree in person_degrees:
        if tools.isNan(person_degree['university_location']) or \
                (tools.isNan(person_degree['started_on']) and
                 not person_degree['degree_completed']):
            continue

        if not person_degree['university_location'] in location_counter:
            location_counter[person_degree['university_location']] = 0

        location_counter[person_degree['university_location']] += 1

        if maxCountry['occurrences'] < location_counter[person_degree['university_location']]:
            maxCountry['country'] = person_degree['university_location']
            maxCountry['occurrences'] = location_counter[person_degree['university_location']]
            if tools.isNan(person_degree['started_on']):
                maxCountry['date'] = "1900/01/01"
            else:
                maxCountry['date'] = person_degree['started_on'].replace("-", "/")
        else:
            if maxCountry['occurrences'] == location_counter[person_degree['university_location']]:
                newDate = person_degree['started_on']
                if tools.isNan(newDate):
                    newDate = "1900-01-01"
                new_started_on = dt.strptime(newDate, "%Y-%m-%d")
                old_started_on = dt.strptime(maxCountry['date'], "%Y/%m/%d")
                if old_started_on > new_started_on or maxCountry['date'] == "1900/01/01":
                    maxCountry['country'] = person_degree['university_location']
                    maxCountry['occurrences'] = location_counter[person_degree['university_location']]
                    maxCountry['date'] = newDate.replace("-", "/")

    if maxCountry['country'] is None and person_jobs:
        for person_job in person_jobs:
            if tools.isNan(person_job['job_location']) or \
                    tools.isNan(person_job['job_start_date']):
                continue

            if not person_job['job_location'] in location_counter:
                location_counter[person_job['job_location']] = 0
            location_counter[person_job['job_location']] += 1

            newDate = person_job['job_start_date']
            new_started_on = dt.strptime(newDate, "%Y-%m-%d")
            old_started_on = dt.strptime(maxCountry['date'], "%Y/%m/%d")
            if old_started_on > new_started_on:
                maxCountry['country'] = person_job['job_location']
                maxCountry['occurrences'] = location_counter[person_job['job_location']]
                maxCountry['date'] = newDate.replace("-", "/")

    return maxCountry['country']
