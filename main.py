import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import zipfile
from glob import glob
import os
from pathlib import Path

JSONS_DIR = "jsons_unzipped"


def find_filenames_by_suffix(suffix='zip'):
    """
    This function finds all the files in the 'results' that end with the given suffix.
    :param suffix:
    :return:
    """
    return [file for file in glob(f"json/*.{suffix}")]


def unzip_all_zip_files():
    filenames = find_filenames_by_suffix()
    for filename in filenames:
        unzip_file(filename)


def unzip_file(filename, target_dir="jsons_unzipped"):
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(target_dir)


def load_json_to_dict(json_path):
    with open(json_path, encoding='utf-8') as json_file:
        try:
            return json.load(json_file)
        except json.decoder.JSONDecodeError as e:
            print(e)
            return None


def calc_num_of_lines(json_dict):
    counter = 0
    for d in json_dict:
        if d.get('type') == 'line-start':
            counter += 1
    return counter


def calc_num_of_words(json_dict):
    counter = 0
    for d in json_dict:
        if d.get('frag'):  # not in [None, '[...]']:
            counter += 1
    return counter


def json_to_csv(json_dict, csv_filename):
    df = pd.DataFrame(json_dict)
    df.to_csv(csv_filename)


def update_dict(d, k1, k2, add=1):
    d.setdefault(k1, dict())
    d[k1][k2] = d[k1].get(k2, 0) + add


def calc_sentences():
    incomplete_sents_counter_by_project, incomplete_sents_counter_general = dict(), dict()
    complete_sents_counter_by_project, complete_sents_counter_general = dict(), dict()
    sents_counter_by_project, sents_counter_general = dict(), dict()

    for directory in os.listdir(JSONS_DIR):
        if directory == "qcat":  # Shitty project
            continue
        incomplete_sents_counter_by_project[directory] = dict()
        complete_sents_counter_by_project[directory] = dict()
        sents_counter_by_project[directory] = dict()

        for path in Path(os.path.join(JSONS_DIR, directory)).rglob('catalogue.json'):
            if str(path) == "jsons_unzipped\cams\catalogue.json":  # multiple projects as subdirectories
                continue
            d = load_json_to_dict(str(path))
            if d and d.get('members'):
                for member in d.get('members').values():
                    lang, period = member.get('language', 'unspecified'), member.get('period', 'unspecified')
                    id_text = member.get('id_text', "") + member.get('id_composite', "")
                    html_dir = "/".join(path.parts[1:-1])
                    url = f"http://oracc.iaas.upenn.edu/{html_dir}/{id_text}/html"
                    print(url)
                    res = requests.get(url)
                    soup = BeautifulSoup(res.text, "html.parser")
                    results = soup.find_all("span", {"class": "cell"})
                    for result in results:
                        is_full = True
                        for content in result.contents:
                            if isinstance(content, str):
                                if "..." in content.strip():  # if words are missing
                                    is_full = False
                                elif content.strip() in [".", "?", "!"]:
                                    if is_full:
                                        update_dict(complete_sents_counter_general, period, lang)
                                        update_dict(complete_sents_counter_by_project[directory], period, lang)
                                    else:
                                        update_dict(incomplete_sents_counter_general, period, lang)
                                        update_dict(incomplete_sents_counter_by_project[directory], period, lang)
                                    update_dict(sents_counter_general, period, lang)
                                    update_dict(sents_counter_by_project[directory], period, lang)
                                    is_full = True

                    print(incomplete_sents_counter_by_project)
                    print(incomplete_sents_counter_general)
        outdir = './results'
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        json_to_csv(complete_sents_counter_general, os.path.join(outdir, "complete_sentences_general.csv"))
        json_to_csv(incomplete_sents_counter_general, os.path.join(outdir, "incomplete_sentences_general.csv"))
        json_to_csv(sents_counter_general, os.path.join(outdir, "sentences_general.csv"))
        for proj_data in sents_counter_by_project:
            json_to_csv(incomplete_sents_counter_by_project[proj_data],
                        os.path.join(outdir, f"incomplete_sentences_{proj_data}.csv"))
            json_to_csv(complete_sents_counter_by_project[proj_data],
                        os.path.join(outdir, f"complete_sentences_{proj_data}.csv"))
            json_to_csv(sents_counter_by_project[proj_data], os.path.join(outdir, f"sentences_{proj_data}.csv"))


def calc_lines():
    words_counter_general, words_counter_by_project = dict(), dict()

    for dir in os.listdir(JSONS_DIR):
        words_counter_general[dir] = dict()
        words_counter_by_project[dir] = dict()
        for path in Path(os.path.join(JSONS_DIR, dir)).rglob('catalogue.json'):
            if str(path) == "jsons_unzipped\cams\catalogue.json":  # multiple projects as subdirectories
                continue
            d = load_json_to_dict(str(path))
            if d and d.get('members'):
                for member in d.get('members').values():
                    id_text = member.get('id_text', "") + member.get('id_composite', "")
                    if os.path.isfile(f'{path.parent}\\corpusjson\\{id_text}.json'):
                        d = load_json_to_dict(f'{path.parent}\\corpusjson\\{id_text}.json')
                        try:
                            json_dict = d['cdl'][0]['cdl'][-1]['cdl'][0]['cdl']
                            num_of_words = calc_num_of_words(json_dict)
                            # num_of_lines = calc_num_of_lines(json_dict)
                        except Exception as e:
                            print(e)
                            continue
                    else:
                        html_dir = "/".join(path.parts[1:-1])
                        url = f"http://oracc.iaas.upenn.edu/{html_dir}/{id_text}/html"
                        # print(url)
                        res = requests.get(url)
                        soup = BeautifulSoup(res.text, "html.parser")
                        num_of_words = len(soup.find_all("a", class_="cbd"))
                        # num_of_lines = len(soup.find_all("span", class_="xlabel"))
                    if num_of_words > 0:
                        lang, period = member.get('language', 'unspecified'), member.get('period', 'unspecified')
                        update_dict(words_counter_general, period, words_counter_general, num_of_words)
                        update_dict(words_counter_general[dir], period, words_counter_general, num_of_words)
                        print(dir, id_text)
                        print(words_counter_general)

    outdir = './results'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    json_to_csv(words_counter_general, os.path.join(outdir, "words_count.csv"))
    for proj_data in words_counter_by_project:
        json_to_csv(words_counter_general[proj_data], os.path.join(outdir, f"words_count_{proj_data}.csv"))


if __name__ == '__main__':
    calc_sentences()
    # calc_lines()
    # total_d, total_d_by_project = dict(), dict()
    # with open("words_json.json", "r") as input_file:
    #     d = json.load(input_file)
    # for key in d:
    #     if key in os.listdir("jsons_unzipped"):
    #         if d[key].keys():
    #             total_d_by_project[key] = d[key]
    #     else:
    #         total_d[key] = d[key]
    # df = pd.DataFrame(total_d)
    # df.to_csv("words_results_table.csv")
    # for proj_data in total_d_by_project:
    #     df = pd.DataFrame(total_d_by_project[proj_data])
    #     df.to_csv(f"words_results_table_{str(proj_data)}.csv")

    # json_file_path = "jsons_unzipped/glass/corpusjson/P282611.json"
    # d = load_json_to_dict(json_file_path)

    # subdirs = [dI for dI in os.listdir(JSONS_DIR) if os.path.isdir(os.path.join('foo', dI))]
    # dirs = os.listdir(JSONS_DIR)
    # with open("results_temp.json") as json_file:
    #     d1 = json.load(json_file)
    # with open("results_temp2.json") as json_file:
    #     d2 = json.load(json_file)
    # with open("results_temp3.json") as json_file:
    #     d3 = json.load(json_file)
    # total_d, total_d_by_project = dict(), dict()
    # ds = [d1, d2, d3]
    # for d in ds:
    #     for key in d:
    #         if key in dirs or key in subdirs:
    #             if total_d_by_project.get(key):
    #                 print("SHOULD NOT HAPPEN!")
    #             if d[key].keys():
    #                 total_d_by_project[key] = d[key]
    #         else:
    #             for sub_key in d[key]:
    #                 total_d[key] = total_d.setdefault(key, dict())
    #                 total_d[key][sub_key] = total_d[key].get(sub_key, 0) + d[key][sub_key]
    #
    # with open("results_by_project.json", "w+") as output_file:
    #     json.dump(total_d_by_project, output_file)
    # with open("results_general.json", "w+") as output_file:
    #     json.dump(total_d, output_file)
    # print(total_d)

    # with open("results_general.json", "r") as output_file:
    #     data = json.load(output_file)
    # df = pd.DataFrame(data)
    # df.to_csv("results_table.csv")
    # with open("results_by_project.json", "r") as output_file:
    #     data = json.load(output_file)
    # for proj_data in data:
    #     df = pd.DataFrame(data[proj_data])
    #     df.to_csv(f"results_table_{str(proj_data)}.csv")

    # json_file_path = "jsons_unzipped/glass/corpusjson/P282611.json"
    # d = load_json_to_dict(json_file_path)
