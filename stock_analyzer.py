from matplotlib import pyplot as plt, offsetbox
import matplotlib.ticker as mticker
import tools
import pandas as pd
import seaborn as sns

'''
def sorted_nested_dict(d):
    if isinstance(d, dict) or isinstance(d, dataextractor.NestedDict):
        return {k: sorted_nested_dict(v) for k, v in sorted(d.items(), reverse=True)}
    else:
        return d
'''


def stockScatter(natStateFilter=None, stockStateFilter=None,
                 natContinentFilter=None, stockContinentFilter=None,
                 color_map="summer_r", saveDir="", source="UN"):
    df_matplot = stocksIntersection(natStateFilter, stockStateFilter, natContinentFilter, stockContinentFilter, source)
    corr = tools.correlation_calc(df_matplot, source_=source)
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(16, 9))

    title = "Migration Stocks"
    if natStateFilter or natContinentFilter:
        if natStateFilter:
            title = title + " from " + natStateFilter
        else:
            title = title + " from " + natContinentFilter
    if stockStateFilter or stockContinentFilter:
        if stockStateFilter:
            title = title + " to " + stockStateFilter
        else:
            title = title + " to " + stockContinentFilter

    # SCATTERPLOT
    p = sns.scatterplot()
    x = 'Crunchbase' if source == "UN" else source
    y = source if source == "UN" else 'Crunchbase'
    sns.regplot(x=x, y=y, data=df_matplot)
    plt.scatter(data=df_matplot, x=x, y=y, c="Crunchbase", cmap=color_map,
                edgecolors="black", linewidths=1, alpha=0.6, s=100)

    cbar = plt.colorbar()
    cbar.set_label("Crunchbase", fontsize=20)
    cbar.ax.tick_params(labelsize=20)
    p.set_xlabel(x, fontsize=20)
    p.set_ylabel(y, fontsize=20)
    # p.set_title(title, fontsize=20)
    loc = "upper right"
    if not saveDir == "original":
        p.set(xscale="log", yscale="log")
        loc = "lower right"
    # CORRELATION BOX
    ob = offsetbox.AnchoredText(corr,
                                loc=loc, borderpad=2.5, prop=dict(size=15))
    ob.patch.set(boxstyle='round, pad=0.6')
    p.add_artist(ob)

    if source == "UN":
        # ADD MAX CRUNCHBASE VALUE
        ordered = df_matplot.sort_values(by="Crunchbase", ascending=False).head(1)
        for el in ordered.values:
            plt.text(el[3], el[4] + (el[4] / 10), el[0] + "->" + el[1] + "\n" + el[2] + "\n", fontdict=dict(size=15))

    p.xaxis.set_major_locator(mticker.FixedLocator(p.get_xticks()))
    p.set_xticklabels([str(int(x)) for x in p.get_xticks()], fontsize=20)

    p.yaxis.set_major_locator(mticker.FixedLocator(p.get_yticks()))
    p.set_yticklabels([str(int(x)) for x in p.get_yticks()], fontsize=20)

    plt.tight_layout()
    if saveDir != "":
        tools.mkdir_p("stocks_pngs/" + saveDir + "/")
        plt.savefig("stocks_pngs/" + saveDir + "/" + title + ".png")
    else:
        plt.show()
    plt.clf()


def stocksIntersection(natStateFilter=None, stockStateFilter=None,
                       natContinentFilter=None, stockContinentFilter=None, source="UN"):
    cont = {}
    if natContinentFilter or stockContinentFilter:
        cont = tools.read_json("Countries/ENA.json")

    stocks_query_file = "CrunchQR\stocks_results.json"
    crunchbase = tools.read_json(stocks_query_file)
    un_stocks_file = "MIMI\Stocks\\UN_stocks.json"
    UN = tools.read_json(un_stocks_file)

    result = tools.common_items(crunchbase, UN, source)

    my_db = []
    for entry in result:
        if (natContinentFilter and entry not in cont[natContinentFilter]) or \
                natStateFilter and entry != natStateFilter:
            continue
        for dest in result[entry]:
            if (stockContinentFilter and dest not in cont[stockContinentFilter]) or \
                    stockStateFilter and dest != stockStateFilter:
                continue
            for year in result[entry][dest]:
                if not result[entry][dest][year]: continue
                my_db.append({'Origin': entry, 'Destination': dest, 'Year': year,
                              'Crunchbase': result[entry][dest][year]['Crunchbase'],
                              source: result[entry][dest][year][source]})
    df = pd.DataFrame(my_db)
    return df
