import json

import tools
import pandas as pd
import seaborn as sns
import matplotlib.ticker as mticker
from matplotlib import pyplot as plt, offsetbox


def flowsIntersection(source, nationality=None, destination=None, aggregate=True, separate_crunchbase=False,
                      del_duplicates=False):
    if source == 'Official':
        estat = tools.read_json("MIMI\Flows\\ESTAT_flows.json")
        UN = tools.read_json("MIMI\Flows\\UN_flows.json")
        official = estat
        for el in UN:
            for de in UN[el]:
                for year1 in UN[el][de]:
                    if el in official:
                        if de in official[el]:
                            official[el][year1] = {}
                            official[el][de][year1] = {"total_cit": int(UN[el][de][year1]["total_cit"]),
                                                       "total_res": int(UN[el][de][year1]["total_res"])}
                        else:
                            official[el][de] = {}
                            official[el][year1] = {}
                            official[el][de][year1] = {"total_cit": int(UN[el][de][year1]["total_cit"]),
                                                       "total_res": int(UN[el][de][year1]["total_res"])}
                    else:
                        official[el] = {}
                        official[el][de] = {}
                        official[el][year1] = {}
                        official[el][de][year1] = {"total_cit": int(UN[el][de][year1]["total_cit"]),
                                                   "total_res": int(UN[el][de][year1]["total_res"])}
    else:
        official = tools.read_json("MIMI\Flows\\" + source + "_flows.json")
    aggregate_countries_file = open("Countries\my_cont.json", "r+")
    aggregate_countries = json.load(aggregate_countries_file)
    query_flows_files = "CrunchQR/flows_results.json"
    crunchbase = tools.read_json(query_flows_files)
    my_db = tools.NestedDict()
    new_crunch_db = None
    if separate_crunchbase:
        crunch_df = {}
        new_crunch_db = []
        for nat in crunchbase:
            if nationality:
                if nat == nationality:
                    first_agg_off = nationality
                else:
                    continue
            else:
                first_agg_off = tools.get_continent(nat, aggregate_countries, aggregate)
            for residence in crunchbase[nat]:
                for dest in crunchbase[nat][residence]:
                    if destination:
                        if dest == destination:
                            third_agg_dest = destination
                        else:
                            continue
                    else:
                        third_agg_dest = tools.get_continent(dest, aggregate_countries, aggregate)
                    for year in crunchbase[nat][residence][dest]:
                        if first_agg_off not in crunch_df:
                            crunch_df[first_agg_off] = {}
                        if third_agg_dest not in crunch_df[first_agg_off]:
                            crunch_df[first_agg_off][third_agg_dest] = {}
                        if year not in crunch_df[first_agg_off][third_agg_dest]:
                            crunch_df[first_agg_off][third_agg_dest][year] = {'cit': 0, 'res': 0}

                        cr_dat = crunchbase[nat][residence][dest][year]
                        crunch_df[first_agg_off][third_agg_dest][year]['cit'] += sum(cr_dat[e] for e in cr_dat)
        for nat in crunchbase:
            for residence in crunchbase[nat]:
                if nationality and residence == nationality:
                    first_agg_off = nationality
                else:
                    first_agg_off = tools.get_continent(residence, aggregate_countries, aggregate)
                for dest in crunchbase[nat][residence]:
                    if destination and dest == destination:
                        third_agg_dest = destination
                    else:
                        third_agg_dest = tools.get_continent(dest, aggregate_countries, aggregate)
                    for year in crunchbase[nat][residence][dest]:
                        if first_agg_off not in crunch_df:
                            crunch_df[first_agg_off] = {}
                        if third_agg_dest not in crunch_df[first_agg_off]:
                            crunch_df[first_agg_off][third_agg_dest] = {}
                        if year not in crunch_df[first_agg_off][third_agg_dest]:
                            crunch_df[first_agg_off][third_agg_dest][year] = {'cit': 0, 'res': 0}

                        cr_dat2 = crunchbase[nat][residence][dest][year]
                        crunch_df[first_agg_off][third_agg_dest][year]['res'] += sum(cr_dat2[j] for j in cr_dat2)

        for First in crunch_df:
            for Second in crunch_df[First]:
                for Year in crunch_df[First][Second]:
                    obj = {'First': First, 'Second': Second, 'Year': Year,
                           'Crunchbase_cit': crunch_df[First][Second][Year]['cit'],
                           'Crunchbase_res': crunch_df[First][Second][Year]['res']}
                    new_crunch_db.append(obj)

    for first_country in official:
        if nationality:
            if first_country == nationality:
                first_agg_off = nationality
            else:
                continue
        else:
            first_agg_off = tools.get_continent(first_country, aggregate_countries, aggregate)
        for second_country in official[first_country]:
            if destination:
                if second_country == destination:
                    second_agg_off = destination
                else:
                    continue
            else:
                second_agg_off = tools.get_continent(second_country, aggregate_countries, aggregate)
            for year in official[first_country][second_country]:
                cr_res_tot = 0
                cr_cit_tot = 0
                for nat in crunchbase:
                    if first_country in crunchbase[nat]:
                        if second_country in crunchbase[nat][first_country] and year in \
                                crunchbase[nat][first_country][second_country]:
                            cr_dat3 = crunchbase[nat][first_country][second_country][year]
                            cr_res_tot = sum(cr_dat3[i] for i in cr_dat3)
                if first_country in crunchbase:
                    for first_job in crunchbase[first_country]:
                        if second_country in crunchbase[first_country][first_job]:
                            if year in crunchbase[first_country][first_job][second_country]:
                                cr_dat4 = crunchbase[first_country][first_job][second_country][year]
                                cr_cit_tot = sum(cr_dat4[k] for k in cr_dat4)

                my_db[first_agg_off][second_agg_off][year][source + "_res"] += int(
                    official[first_country][second_country][year]['total_res'])
                my_db[first_agg_off][second_agg_off][year][source + "_cit"] += int(
                    official[first_country][second_country][year]['total_cit'])
                my_db[first_agg_off][second_agg_off][year]["Crunchbase_res"] += cr_res_tot
                my_db[first_agg_off][second_agg_off][year]["Crunchbase_cit"] += cr_cit_tot

    new_db = []
    for cont in my_db:
        for sec_cont in my_db[cont]:
            if cont == sec_cont and del_duplicates:
                continue
            for year in my_db[cont][sec_cont]:
                obj = {"First": cont, "Second": sec_cont, "Year": year,
                       "" + source + "_res": my_db[cont][sec_cont][year][source + "_res"],
                       "" + source + "_cit": my_db[cont][sec_cont][year][source + "_cit"],
                       "Crunchbase_res": my_db[cont][sec_cont][year]["Crunchbase_res"],
                       "Crunchbase_cit": my_db[cont][sec_cont][year]["Crunchbase_cit"]
                       }
                new_db.append(obj)

    db1 = pd.DataFrame(new_db)
    if separate_crunchbase:
        db2 = pd.DataFrame(new_crunch_db)
        return db1, db2
    return db1


def crunchbase_flows_matplot(source, aggregate=False, del_duplicates=False, natStateFilter=None, destStateFilter=None,
                             my_color_map="inferno", save=False):
    df = flowsIntersection(source, aggregate=aggregate,
                           del_duplicates=del_duplicates,
                           nationality=natStateFilter,
                           destination=destStateFilter)
    citizen_corr, resident_corr = tools.correlation_calc(df, source, flows=True)
    plot_see(source, df, citizen_corr, resident_corr,
             my_color_map=my_color_map, aggregate=aggregate,
             natStateFilter=natStateFilter, destStateFilter=destStateFilter, save=save)


def plot_see(source, df, cit_corr, res_corr, my_color_map="inferno",
             aggregate=False, natStateFilter=None, destStateFilter=None, save=False):
    for flowsType in sorted(["_cit", "_res"]):
        temp_df = df
        plt.clf()

        sns.set_theme(style="whitegrid")
        plt.figure(figsize=(16, 9))

        if not temp_df["Crunchbase" + flowsType].any():
            return
        # temp_df = temp_df.groupby(by=['First', 'Second'])[
        # "Crunchbase" + flowsType, source + flowsType].sum().reset_index()

        path = "flows_pngs/"
        if aggregate:
            path += "aggregated/"
            agg = "aggregated"
        else:
            path += "original/"
            agg = ""
        fil = ""
        if natStateFilter:
            path += "filtered_from_" + natStateFilter + "/"
            fil = "filtered from " + natStateFilter
        if destStateFilter:
            path += "filtered_to_" + destStateFilter + "/"
            fil = "filtered to " + destStateFilter
        title = "Migration Flows " + source + " " + agg + " " + fil

        # SCATTERPLOT
        p = sns.scatterplot()
        plt.scatter(data=temp_df, x="Crunchbase" + flowsType, y=source + flowsType, c="Crunchbase" + flowsType,
                    cmap=my_color_map, edgecolors="black", linewidths=1, alpha=0.6, s=200)
        cbar = plt.colorbar()
        cbar.set_label("Crunchbase" + flowsType, fontsize=20)
        cbar.ax.tick_params(labelsize=20)
        p.set_xlabel("Crunchbase" + flowsType, fontsize=20)
        p.set_ylabel(source + flowsType, fontsize=20)
        # p.set_title(title, fontsize=20)
        p.set(xscale="log", yscale="log")
        # CORRELATION BOX
        ob = offsetbox.AnchoredText(cit_corr if flowsType == "_cit" else res_corr, loc="lower right", borderpad=2.5, prop=dict(size=15))
        ob.patch.set(boxstyle='round, pad=0.6', alpha=0.5)
        p.add_artist(ob)

        p.xaxis.set_major_locator(mticker.FixedLocator(p.get_xticks()))
        p.set_xticklabels([str(int(x)) for x in p.get_xticks()], fontsize=20)

        p.yaxis.set_major_locator(mticker.FixedLocator(p.get_yticks()))
        p.set_yticklabels([str(int(x)) for x in p.get_yticks()], fontsize=20)

        plt.tight_layout()
        if save:
            tools.mkdir_p(path)
            plt.savefig(path + source + flowsType + "_" + str(aggregate) + ".png")
        else:
            plt.show()
