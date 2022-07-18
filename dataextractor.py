import json
import pandas as pd
from datetime import datetime as dt
from alive_progress import alive_bar
import tools
import userbase_analyzer


def query_flows(total=None, save=False, counter=0, natFromJobs=False):
    my_db = tools.NestedDict()
    if total is None:
        total = tools.read_total()
    if counter == 0:
        counter = count_jobs(total)
    with alive_bar(counter, title="Flows",
                   force_tty=True, spinner="classic") as flows_bar:
        for person in total:
            if natFromJobs:
                pers_jobs = total[person]['jobs']
            else:
                pers_jobs = None
            if not len(total[person]["jobs"]) or \
                    not (ed_country := userbase_analyzer.get_nationality(total[person]['degrees'], pers_jobs)):
                continue
            for person_job in total[person]['jobs']:
                flows_bar()
                if tools.isNan(person_job['job_location']) or \
                        tools.isNan(person_job['job_start_date']) or \
                        tools.isNan(person_job['job_end_date']):
                    continue

                job_loc = person_job['job_location']
                job_start = dt.strptime(person_job['job_start_date'], "%Y-%m-%d")
                job_end = dt.strptime(person_job['job_end_date'], "%Y-%m-%d")

                for person_second_job in total[person]['jobs']:
                    if person_job == person_second_job or \
                            tools.isNan(person_second_job['job_start_date']) or \
                            tools.isNan(person_second_job['job_location']) or \
                            (tools.isNan(person_second_job['job_end_date']) and
                             not person_second_job['job_is_current']):
                        continue
                    second_job_loc = person_second_job['job_location']
                    second_job_start = dt.strptime(person_second_job['job_start_date'], "%Y-%m-%d")

                    for year in range(2009, 2023):
                        year_start = dt.strptime(str(year), "%Y")
                        year_end = dt.strptime("" + str(year) + "/12/31", "%Y/%m/%d")
                        if year_start < job_start or not (year_end > job_end > year_start) or \
                                not (year_start < second_job_start < year_end):
                            continue

                        gender = gender_parse(total[person]['person_gender'])
                        my_db[ed_country][job_loc][second_job_loc][str(year)][gender] += 1
    if save:
        tools.mkdir_p("CrunchQR/")
        file_name = "CrunchQR/flows_results.json"
        flows_file = open(file_name, "w+")
        flows_file.write(json.dumps(my_db, indent=4))
    return my_db


def query_stocks(total=None, save=False, counter=0, natFromJobs=False):
    my_db = tools.NestedDict()
    if total is None:
        total = tools.read_total()
    if counter == 0:
        counter = count_jobs(total)

    with alive_bar(counter, title="Stocks", force_tty=True, spinner="classic") as stocks_bar:
        for person in total:
            if natFromJobs:
                pers_jobs = total[person]['jobs']
            else:
                pers_jobs = None
            if not len(total[person]['jobs']) or not (
            ed_country := userbase_analyzer.get_nationality(total[person]['degrees'], pers_jobs)):
                continue
            for person_job in total[person]['jobs']:
                stocks_bar()
                if tools.isNan(person_job['job_location']) or tools.isNan(person_job['job_start_date']):
                    continue

                job_start = dt.strptime(person_job['job_start_date'], "%Y-%m-%d")
                job_loc = person_job['job_location']
                if tools.isNan(person_job['job_end_date']):
                    if person_job['job_is_current']:
                        job_end = dt.strptime("2023/01/01", "%Y/%m/%d")
                    else:
                        continue
                else:
                    job_end = dt.strptime(person_job['job_end_date'], "%Y-%m-%d")

                for year in range(2009, 2023):
                    year_t = dt.strptime(str(year), "%Y")
                    if year_t < job_start or year_t > job_end:
                        continue

                    gender = gender_parse(total[person]['person_gender'])
                    my_db[ed_country][job_loc][str(year)][gender] += 1

    if save:
        tools.mkdir_p("CrunchQR/")
        name = "CrunchQR/stocks_results"
        fileName = name + ".json"
        stockFile = open(fileName, "w+")
        stockFile.write(json.dumps(my_db, indent=4))
    return my_db


def total_create(jobs_file=None, degrees_file=None, bulk_dir=None, save=False):
    all_people = people_parsing(bulk_dir=bulk_dir)
    activities = organization_parsing(bulk_dir=bulk_dir)

    if jobs_file is not None:
        job_file = open(jobs_file, "r+")
        jobs = json.load(job_file)
    else:
        jobs = jobs_parsing(all_people=all_people, activities=activities, bulk_dir=bulk_dir, save=save)
    my_persona_db = {}

    if degrees_file is not None:
        degree_file = open(degrees_file, "r+")
        degrees = json.load(degree_file)
    else:
        degrees = degree_parsing(all_people=all_people, activities=activities, bulk_dir=bulk_dir, save=save)

    with alive_bar(len(jobs) + len(degrees), title="Total", force_tty=True, spinner="classic") as Total_bar:
        for each in jobs:
            if jobs[each]["job_person_uuid"] not in my_persona_db:
                my_persona_db[jobs[each]["job_person_uuid"]] = {
                    'person_gender': jobs[each]['person_gender'],
                    'person_location': jobs[each]['person_location'],
                    'person_account_creation': jobs[each]['creation_date'],
                    'jobs': [],
                    'degrees': []
                }
            jobp = jobs[each]
            my_persona_db[jobp['job_person_uuid']]['jobs'].append(jobp)
            Total_bar()

        for each_deg in degrees:
            if degrees[each_deg]["person_degree_uuid"] not in my_persona_db:
                my_persona_db[degrees[each_deg]["person_degree_uuid"]] = {
                    'person_gender': degrees[each_deg]['person_gender'],
                    'person_location': degrees[each_deg]['person_location'],
                    'person_account_creation': degrees[each_deg]['creation_date'],
                    'jobs': [],
                    'degrees': []
                }
            degp = degrees[each_deg]
            my_persona_db[degp['person_degree_uuid']]['degrees'].append(degp)
            Total_bar()

    if save:

        tools.mkdir_p("CrunchBase Data/")
        with open("CrunchBase Data/total3.json", "w+") as outFile:
            outFile.write(json.dumps(my_persona_db, indent=4))

    return my_persona_db


def degree_parsing(all_people=None, activities=None, bulk_dir=None, save=False):
    if all_people is None:
        all_people = people_parsing(bulk_dir)
    if activities is None:
        activities = organization_parsing(bulk_dir)
    if bulk_dir is None:
        bulk_dir = "bulk_dir"
    degree_file = pd.read_csv(r'' + bulk_dir + "degrees.csv")
    degreesDataFrame = pd.DataFrame(degree_file)
    my_degrees = {}
    with alive_bar(len(degreesDataFrame.values), title="Degrees", force_tty=True) as degrees_bar:
        for degree in degreesDataFrame.values:
            degree_uuid = degree[0]
            person_degree_uuid = degree[8]
            university_degree_uuid = degree[10]
            degree_completed = degree[16]
            degree_start = degree[14]
            degree_type = degree[12]
            person_gender = ''
            person_location = ''
            university_location = ''
            if person_degree_uuid in all_people:
                if all_people[person_degree_uuid]:
                    person_gender = all_people[person_degree_uuid].get('gender')
                    person_location = all_people[person_degree_uuid].get('location')
                    person_account_creation = all_people[person_degree_uuid].get('creation_date')
            else:
                continue
            if university_degree_uuid in activities:
                if activities[university_degree_uuid]:
                    university_location = activities[university_degree_uuid].get('organization_country')
            else:
                continue
            my_degrees[degree_uuid] = {'person_degree_uuid': person_degree_uuid,
                                       'university_degree_uuid': university_degree_uuid,
                                       'creation_date': person_account_creation,
                                       'degree_completed': degree_completed, 'person_gender': person_gender,
                                       'person_location': person_location, 'university_location': university_location,
                                       'started_on': degree_start, 'degree_type': degree_type}
            degrees_bar()
        if save:
            tools.mkdir_p("CrunchBase Data/")
            with open("CrunchBase Data/degrees2.json", "w+") as file:
                file.write(json.dumps(my_degrees, indent=4))
    return my_degrees


def jobs_parsing(all_people=None, activities=None, bulk_dir=None, save=False):
    if bulk_dir is None:
        bulk_dir = "bulk_dir"
    if all_people is None:
        all_people = people_parsing(bulk_dir=bulk_dir)
    if activities is None:
        activities = organization_parsing(bulk_dir=bulk_dir)

    jobs_file = pd.read_csv(r'' + bulk_dir + "jobs.csv")
    jobsDataFrame = pd.DataFrame(jobs_file)
    my_jobs = {}
    with alive_bar(len(jobsDataFrame.values), title="Jobs", force_tty=True) as jobs_bar:
        for job in jobsDataFrame.values:
            job_uuid = job[0]
            job_person_uuid = job[8]
            job_organization_uuid = job[10]
            job_start_date = job[12]
            job_end_date = job[13]
            job_is_current = job[14]
            job_title = job[15]
            job_type = job[16]
            job_location = ''
            person_gender = ''
            person_location = ''

            if job_person_uuid in all_people:
                person_gender = all_people[job_person_uuid].get('gender')
                person_location = all_people[job_person_uuid].get('location')
                person_account_creation = all_people[job_person_uuid].get('creation_date')

            if job_organization_uuid in activities:
                job_location = activities[job_organization_uuid].get('organization_country')
            my_jobs[job_uuid] = {'job_person_uuid': job_person_uuid, 'job_organization_uuid': job_organization_uuid,
                                 'job_start_date': job_start_date, 'job_end_date': job_end_date,
                                 'job_is_current': job_is_current, 'job_location': job_location,
                                 'person_gender': person_gender, 'person_location': person_location,
                                 'creation_date': person_account_creation,
                                 'job_title': job_title, 'job_type': job_type}
            jobs_bar()
        if save:
            tools.mkdir_p("CrunchBase Data/")
            with open("CrunchBase Data/jobs2.json", "w+") as jobs_file:
                jobs_file.write(json.dumps(my_jobs, indent=4))
    return my_jobs


def people_parsing(bulk_dir):
    people_file = pd.read_csv(r'' + bulk_dir + "people.csv")
    peopleDataFrame = pd.DataFrame(people_file)
    all_people = {}
    with alive_bar(len(peopleDataFrame.values), title="Person", force_tty=True) as person_bar:
        for person in peopleDataFrame.values:
            person_uuid = person[0]
            person_gender = person[10]
            person_location = person[11]
            person_actual_job = person[15]
            person_account_creation = person[6]
            all_people[person_uuid] = {'gender': person_gender,
                                       'location': person_location,
                                       'creation_date': person_account_creation}
            person_bar()

    return all_people


def organization_parsing(bulk_dir):
    organization_file = pd.read_csv(r'' + bulk_dir + "organizations.csv")
    organizationsDataFrame = pd.DataFrame(organization_file)
    activities = {}
    with alive_bar(len(organizationsDataFrame.values), title="Organizations", force_tty=True) as organization_bar:
        for organization in organizationsDataFrame.values:
            organization_uuid = organization[0]
            organization_country = organization[12]
            activities[organization_uuid] = {'organization_country': organization_country}
            organization_bar()

    return activities


def gender_parse(gender):
    if gender == 'male' or gender == 'female':
        return gender
    return 'unknown'


def count_jobs(total, person_jobs=True):
    counter = 0
    for person in total:
        if person_jobs:
            pers_jobs = total[person]['jobs']
        else:
            pers_jobs = None
        if not userbase_analyzer.get_nationality(total[person]['degrees'], pers_jobs):
            continue
        counter += len(total[person]['jobs'])
    return counter


def degree_jobs_counter(total=None, withGender=False):
    if not total:
        total = tools.read_total()
    my_degrees = {}
    my_jobs = {}
    for j in ["male", "female", "unknown", "total"]:
        if j not in my_jobs:
            my_jobs[j] = {}
        if j not in my_degrees:
            my_degrees[j] = {}
        for i in sorted(range(1, 101)):
            my_jobs[j][i] = 0
            my_degrees[j][i] = 0
    year_start = dt.strptime("2010/1/1", "%Y/%m/%d")
    for person in total:
        job_counter = 0

        if "jobs" not in total[person] or "degrees" not in total[person]:
            continue

        for j in total[person]["jobs"]:
            if tools.isNan(j['job_start_date']):
                continue
            else:
                job_start = dt.strptime(j['job_start_date'], "%Y-%m-%d")
            if tools.isNan(j['job_end_date']):
                if not j['job_is_current']:
                    continue
                job_end = dt.strptime("2023/1/1", "%Y/%m/%d")
            else:
                job_end = dt.strptime(j['job_end_date'], "%Y-%m-%d")
            if job_start > year_start or job_end > year_start or j["job_is_current"]:
                job_counter += 1

        gender = gender_parse(total[person]['person_gender'])
        if len(total[person]["degrees"]) > 0:
            my_degrees[gender][len(total[person]["degrees"])] += 1
            my_degrees["total"][len(total[person]["degrees"])] += 1
        if job_counter > 0:
            my_jobs[gender][job_counter] += 1
            my_jobs["total"][job_counter] += 1
    tools.mkdir_p("CrunchQR/")
    with open("CrunchQR/degrees_counter.json", "w+") as results:
        results.write(json.dumps(my_degrees, indent=4))

    with open("CrunchQR/jobs_counter.json", "w+") as results2:
        results2.write(json.dumps(my_jobs, indent=4))

