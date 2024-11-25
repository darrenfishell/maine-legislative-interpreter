from thefuzz import process, fuzz
import pandas as pd
import re
import os
import requests
import geopandas as gpd
from pyzipcode import ZipCodeDatabase
import dotenv
import json

config = dotenv.dotenv_values()

# constants
TOWNS = gpd.read_file("https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/Maine_Town_and_Townships_Boundary_Polygons_Dissolved/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson")
TOWN_LIST = TOWNS.TOWN.to_list()
OPENSTATES_BASE_URL = "https://v3.openstates.org/bills?jurisdiction=Maine&session={}&identifier={}&sort=updated_desc&page=1&per_page=10&apikey={}"

# shared instances
zcdb = ZipCodeDatabase()
s = requests.Session()

def get_text(filepath, prefix=None):
    if prefix:
        filepath = os.path.join(prefix, filepath)
    # read in the text of the bill
    with open(filepath, 'r') as file:
        text = file.readlines()
    return text

def get_members():
    # read the list of members from the CSV
    members = pd.read_csv("./member_list.csv", index_col=0)
    # create a match string column in the dataframe of members
    members["match_str"] = members["title"] + " " + members["last"] + " " + members["town"].fillna("") + members["county"].fillna("")
    return members

def fuzzy_match_member(match_str, members=None):
    """
    This function uses fuzzy matching to identify the name of the bill sponsor
    from the member list that best matches the name extracted from the bill text.
    """
    if members is None:
        members = get_members()
        
    # get the best fuzzy match
    match, score = process.extractOne(
        match_str, members["match_str"].to_list(), 
        scorer=fuzz.token_set_ratio)
    # print a warning if the match score is below 80
    if score < 80:
        print("Could not match: {}\t{}".format(match_str, match))
        return pd.NA
    
    # return the index of the members dataframe that matches
    return members[members.match_str == match].index[0]

def match_one_member(text, members=None):
    if members is None:
        members = get_members()
    
    # find the line listing the bill's sponsor
    for line in text:
        sponsor = re.match("Presented by ([A-Za-z]+) ([A-Za-z\']+) of ([A-Za-z]+)", line)
        if sponsor:
            break
    
    if sponsor is None:
        return None
    
    # make a string containing the sponsor information and correct capitalization
    role, last, town = sponsor.groups()
    match_str = (role[0:3]+" "+last+" "+town).title()
    return fuzzy_match_member(match_str, members)

def add_members(df):
    members = get_members()
    df["member"] = df["text"].apply(match_one_member, members=members)
    return df

def add_ld(df):
    df["ldnumber"] = df["ld"].apply(lambda x: int(x.split("-")[2]))
    return df

def list_bills(directory="../../mathieu-maine-bills/data/131/bills/"):
    """
    Returns a python list of bills
    """
    # get a list of everything in the provided directory
    sdir = os.scandir(directory)
    # return a list of files
    bill_list = pd.DataFrame.from_records([{
        "file": d.name, 
        "ld_ext": d.name.rsplit(".", maxsplit=1)[0],
        } for d in sdir if d.name.lower().endswith(".txt")])
    # add the LD number
    bill_list["ld"] = bill_list["ld_ext"].apply(lambda d: int(d.split("-")[2]))
    # add the session
    bill_list["session"] = bill_list["ld_ext"].apply(lambda d: d.split("-")[0])
    # add the amendment
    bill_list["amendment"] = bill_list["ld_ext"].apply(
        lambda d: d.split("-", maxsplit=3)[-1] if len(d.split("-", maxsplit=3)) > 3 else None
    )
    # add the text of the bill
    bill_list["text"] = bill_list["file"].apply(get_text, prefix=directory)
    # reorder columns and return
    return bill_list[['session', 'ld', 'amendment', 'text', 'ld_ext', 'file']]

def list_bills_with_members(**kwargs):
    bills = list_bills(**kwargs)
    bills = add_members(bills)
    members = get_members()
    return bills.merge(
        members,
        how="left",
        left_on="member",
        right_index=True,
        suffixes=(None, "_sponsor")
        )

def get_bill_info(ldno, directory="../data/131/legiscan/bill/"):
    try:
        return json.load(open(os.path.join(directory, f"LD{int(ldno)}.json")))
    except FileNotFoundError:
        print(f"Could not find bill info for LD{int(ldno)}")
        return None
    
def get_bill_attr(ldno, attr="title", directory="../data/131/legiscan/bill/"):
    try:
        info = json.load(open(os.path.join(directory, f"LD{int(ldno)}.json")))
        if info:
            return info["bill"][attr]
    except:
        return None

def list_testimony(directory="../data/131/testimony/"):
    """
    Returns a dataframe of testimony
    """
    all_files = []
    # get a list of everything in the provided directory
    for root, dirs, files in os.walk(directory):
        if len(files) > 0:
            all_files.extend(os.path.join(root, f) for f in files)
    all_files = [{
            "ld": int(f.split("/")[-2].split("-")[2]),
            "name": f.split("/")[-1].split(".")[0].split("(")[0].strip(),
            "organization": f.split("/")[-1].split("(")[1].split(")")[0].strip(),
            "text": get_text(f),
            "file": f
        } for f in all_files if f.lower().endswith(".txt")]
    # return a list of files
    testimony_list = pd.DataFrame.from_records(all_files)
    return testimony_list

def fuzzy_match_towns(testimony):
    """
    This function takes a row of a dataframe and returns the town that best matches
    the "organization" column.
    """
    # filter out rows with no organization
    if testimony == "None" or testimony == "":
        return None
    # check if organization is a zip code
    if re.match("^\d{5}$", testimony):
        zc = zcdb[testimony]
        if zc.state == "ME":
            return zc.city
        else:
            return None
    # remove non-alphanumeric characters
    testimony = re.sub("[^A-Za-z0-9 ]", "", testimony)
    # remove state from end of string
    testimony = re.sub(" ME$", "", testimony)
    testimony = re.sub(" Maine$", "", testimony)
    # get the best fuzzy match
    match, score = process.extractOne(
        testimony, 
        TOWN_LIST, 
        scorer=fuzz.token_set_ratio)
    # assume a match score below 80 is not a match
    if score < 90:
        return None
    return match