from stock_analyzer import stockScatter
from flows_analyzer import crunchbase_flows_matplot

import dataextractor


def print_hi():

    export_dir = "your bulk export dir"
    total = dataextractor.total_create(save=False, bulk_dir=export_dir)
    dataextractor.query_stocks(total, save=True, natFromJobs=True)
    dataextractor.query_flows(total, save=True, natFromJobs=True)

    # Dati in scala logaritmica from italy
    stockScatter(color_map="viridis")
    # Dati in scala logaritmica from italy
    stockScatter(color_map="viridis", saveDir="logaritmic")
    # Dati in scala logaritmica from italy
    stockScatter(natStateFilter="ITA", color_map="flare", saveDir="ITA")
    # Dati in scala logaritmica to italy
    stockScatter(stockStateFilter="ITA", color_map="inferno_r", saveDir="ITA")

    # Dati in scala logaritmica from GBR
    stockScatter(natStateFilter="GBR", color_map="flare", saveDir="GBR")
    # Dati in scala logaritmica to GBR
    stockScatter(stockStateFilter="GBR", color_map="inferno_r", saveDir="GBR")

    # Dati in scala logaritmica from North America to Europe
    stockScatter(natContinentFilter="North America", stockContinentFilter="Europe",
                                color_map="viridis", saveDir="NAmerica_Europe")
    # Dati in scala logaritmica from Europe to North America
    stockScatter(stockContinentFilter="North America", natContinentFilter="Europe",
                                color_map="viridis", saveDir="NAmerica_Europe")
    #### Flows Scatter Plot
    # Flows UN senza alcuna modifica
    crunchbase_flows_matplot("UN", save=True)
    # Flows ESTAT senza alcuna modifica
    crunchbase_flows_matplot("ESTAT", save=True)
    # Flows UN aggregati
    crunchbase_flows_matplot("UN", aggregate=True, save=True, my_color_map="viridis")
    # Flows ESTAT aggregati
    crunchbase_flows_matplot("ESTAT", aggregate=True, save=True, my_color_map="viridis")
    # Flows ESTAT aggregati dall'Italia verso l'estero
    crunchbase_flows_matplot("ESTAT", aggregate=True, natStateFilter="ITA", save=True, my_color_map="turbo")
    # Flows ESTAT aggregati dall'estero verso l'Italia
    crunchbase_flows_matplot("ESTAT", aggregate=True, destStateFilter="ITA", save=True, my_color_map="rainbow")
    # Flows UN unito Eurostat aggregati dallo United Kingdom verso l'estero
    crunchbase_flows_matplot("Official", aggregate=True, natStateFilter="GBR", save=True, my_color_map="gist_rainbow")
    # Flows UN unito Eurostat aggregati dall'estero verso lo United Kingdom
    crunchbase_flows_matplot("Official", aggregate=True, destStateFilter="GBR", save=True, my_color_map="tab20c")

    # Flows UN unito Eurostat aggregati
    crunchbase_flows_matplot("Official", aggregate=True, save=True, my_color_map="cool")
    # Flows UN unito Eurostat aggregati
    crunchbase_flows_matplot("Official", aggregate=True, save=True, my_color_map="summer")









if __name__ == '__main__':
    print_hi()

