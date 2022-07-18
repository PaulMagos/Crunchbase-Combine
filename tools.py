import json
import numpy as np

# Checks if everything in num is in [NaN, nan, Nan]
from functools import total_ordering

import pycountry


def isNan(num):
    if num in ["nan", "NaN", "Nan"]:
        return True
    return num != num


# Returns the total file with all Crunchbase users profiles
def read_total(total_file=None):
    if total_file is None:
        return json.load(open("CrunchBase Data/total3.json", "r+"))
    return json.load(open(total_file, "r+"))


# Returns an opened json file
def read_json(file):
    return json.load(open(file, "r+"))


# https://stackoverflow.com/a/31809973
def mkdir_p(mypath):
    '''Creates a directory. equivalent to using mkdir -p on the command line'''

    from errno import EEXIST
    from os import makedirs, path

    try:
        makedirs(mypath)
    except OSError as exc:  # Python >2.5
        if exc.errno == EEXIST and path.isdir(mypath):
            pass
        else:
            raise


# Nested dict, auto-creates the tree by the key you pass
@total_ordering
class NestedDict(dict):
    def __getitem__(self, key):
        if key not in self and (
                key == 'male' or key == 'female' or key == 'unknown' or
                key == 'Value' or key == 'total_cit' or key == 'total_res' or
                key == 'Official_cit' or key == 'Official_res' or key == "Crunchbase_cit" or
                key == "Crunchbase_res" or key == "ESTAT_cit" or key == "ESTAT_res" or key == "UN_cit" or
                key == "UN_res" or key == "UN" or key == "ESTAT" or key == "Crunchbase"):
            return 0
        if key in self:
            return self.get(key)
        return self.setdefault(key, NestedDict())


# Gets state iso3 code
def getIso3(state):
    for i in list(pycountry.countries):
        if i == state:
            return i.alpha_3
    return None


# Gets state iso2 code by iso3 code
def getIso2(string):
    list_alpha_2 = [i.alpha_2 for i in list(pycountry.countries)]
    list_alpha_3 = [i.alpha_3 for i in list(pycountry.countries)]
    if len(string) == 2 and string in list_alpha_2:
        return pycountry.countries.get(alpha_2=string).alpha_2
    elif len(string) == 3 and string in list_alpha_3:
        return pycountry.countries.get(alpha_3=string).alpha_2
    else:
        return "NaN"


# Gets common keys in d1 and d2, and creates a new dictionary (Intersection)
def common_items(d1, d2, source="UN"):
    return {k: common_items(d1[k], d2[k], source) if (k not in [f"{i:d}" for i in sorted(range(2010, 2021))])
                                                     and isinstance(d1[k], dict) else
    {'Crunchbase': sum(d1[k][e] for e in d1[k]), source: int(d2[k]['total'])}
            for k in (d1.keys() & d2.keys())}


# Returns subcontinent of country, by taking it from aggregate countries arg
def get_continent(country, aggregate_countries, aggregate=True):
    if not aggregate:
        return country
    for cont in aggregate_countries:
        if country == "ROM":
            country = "ROU"
        if country == "BAH":
            country = "BHS"
        if country in aggregate_countries[cont]:
            return cont
    return country


def correlation_calc(df, source_="UN", crunch_="Crunchbase", flows=None):
    crunch = crunch_
    source = source_
    out = {}
    purePears = "Pearson"
    PearsLog = "PearsonLog"
    PearsLogCB = "PearsonLogCB"
    PearsLogOf = "PearsonLog" + source
    pureSpear = "Spearman"
    el = "_cit" if flows else ""
    while True:
        if df[crunch + el].any() and df[source + el].any():
            np.seterr(divide='ignore')
            maCrunch = np.ma.masked_invalid(df[crunch + el])
            maOff = np.ma.masked_invalid(df[source + el])
            logMaCrunch = np.log(maCrunch)
            logMaOff = np.log(maOff)
            out[purePears + el] = round(
                np.ma.corrcoef(maCrunch, maOff)[0][1], 2)
            out[PearsLog + el] = round(
                np.ma.corrcoef(logMaCrunch, logMaOff)[0][1], 2)
            out[PearsLogOf + el] = round(
                np.ma.corrcoef(maCrunch, logMaOff)[0][1], 2)
            out[PearsLogCB + el] = round(
                np.ma.corrcoef(logMaCrunch, maOff)[0][1], 2)

            df_corr_spearman = df.corr(method='spearman')
            out[pureSpear + el] = round(
                df_corr_spearman[crunch + el][source + el], 2)
            if flows:
                if el == "_cit":
                    el = "_res"
                    continue
            break
    np.seterr(divide='warn')

    if flows:
        new_d = "\n".join("{}: {!r}".format(k, out[k])
                          for k in sorted(out, key=len, reverse=False) if k.endswith("_cit"))
        new_d2 = "\n".join("{}: {!r}".format(k, out[k])
                           for k in sorted(out, key=len, reverse=False) if k.endswith("_res"))
        return new_d, new_d2

    return "\n".join("{}: {!r}".format(k, out[k])
                     for k in sorted(out, key=len, reverse=False))
