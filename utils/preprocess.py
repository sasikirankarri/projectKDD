import json
import re
import pandas as pd
import sparknlp

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col,udf,explode , split

from pyspark.sql.types import MapType, StringType, IntegerType, ArrayType 
import pandas as pd
import time
from neo4j import GraphDatabase, basic_auth
import time
from tqdm import tqdm
from urllib.request import urlopen
import urllib.request
from datetime import datetime

def cleanInfoBox(answer):
  infoboxDict = {}
  try:
    x=answer.index("{{Infobox")
    y=answer.index("'''")
    answer = answer[x:y].replace("\n","")
    answer = re.sub(r"<ref.*<\/ref>|<ref.*?\/>|\*|\;","",answer)
    #answer = re.sub(r"<ref.*?\/>|\*|\;","",answer)
    answer = re.sub(r"\s\s+|&nbsp"," ",answer)
    #answer = re.sub(r"&nbsp"," ",answer)
    answer = re.sub(r" = ","=",answer)
    answer = re.sub(r"\<\!\-\-.*?\-\-\>","",answer)
    #answer = re.sub(r"(?<=\[\[)(.*?)(?=\]\])", "\\1,",answer)
    answer = re.sub(r"\[\[(.*?)\]\]", "\\1,",answer)
    answer = re.sub(r"={{(.*?)\|(.*?)}}", "=\\2,",answer)
    answer = re.sub(r"{|}|<br />|<br/>|<br>","",answer)
    #answer = re.sub(r"(\|+.*?=)","#-\\1",answer)
    def replacePipe(answer):
      answer = re.sub(r"(\|.[^=]*?\|)","#-\\1",answer)
      res = re.subn(r"#-\|"," ",answer)
      answer=res[0]
      if res[1]>0:
        replacePipe(answer)
    answer = re.sub(r"\|\|","|",answer)
    infoBox = answer.split("|")
    for item in infoBox:
      if "=" in item:
        key = item.split("=")[0].strip(' ')
        value = item.split("=")[1].rstrip(',').strip(' ')
        if "," in value:
          value = list(filter(None, value.split(",")))
        if len(value)>0:
          infoboxDict[key] = value
    infoboxDict['responseCode'] = "Success"
    return infoboxDict
  except:
    infoboxDict['responseCode'] = "Parsing error"
    return infoboxDict

"""
def getPageIndexinCat(category,wikiFlag, continueFlag, gcmcontinue,list_pageIndex):
    list_local = list_pageIndex
    url = "https://<WIKI_FLAG>.wikipedia.org/w/api.php?action=query&generator=categorymembers&gcmlimit=500&gcmtitle=Category:"
    url = url.replace("<WIKI_FLAG>",wikiFlag)
    url= "".join([url,category])             
    if(continueFlag == "TRUE"):
      url= "".join([url,"&continue=gcmcontinue||&gcmcontinue="])
      url= "".join([url,gcmcontinue])
    
    url = "".join([url,"&format=json"])
    jsonData = urlopen(url).read()
    json_object = json.loads(jsonData)

    try:
      for pageindex in json_object['query']['pages']:
          list_local.append(pageindex)
      try:
        if json_object['continue'] != "":
          getPageIndexinCat(category,wikiFlag, "TRUE", json_object['continue']['gcmcontinue'],list_local)
      except:
        print("{} - Completed fetching page index for category : {}, Records found: {}".format(datetime.now(), category,len(list_local)))

      #print("{} - Fetching page index for category : {}, Records found: {}".format(datetime.now(), category,len(list_local)))
    except:
      print("{} - ERROR No record found - Fetching page index for category : {}, Records found: {}".format(datetime.now(), category,len(list_local)))
    #index_rdd = spark.sparkContext.parallelize(list_local)
    #index_rdd.take(5)
    return list_local
"""


def extractMovieEntity(info):

    Movie_attrib_list = ['name','budget','released','runtime','gross']
    
    MovieDict ={}
    
    try :
        MovieDict =  {k:info[k] for k in Movie_attrib_list if k in info }
    except:
        MovieDict['responseCode'] = "Parsing error"
    return MovieDict

def extractPersonRelation(info):

    person_role_list= ['starring','producer','writer','director','music']
  
    PersonRoleArray = []
    PersonNameArray = []
  
    result_array = []
    for k in person_role_list:
    
        if k in info:
            dictValue = info[k]

            if (~isinstance(dictValue,str)) & (len(dictValue) > 0):

                for value in dictValue:
                    PersonRoleArray.append(k)
                    PersonNameArray.append(value)
                    result_array.append(k+"~"+value.strip())
            ## otherwise assign the value
            else:
                PersonRoleArray.append(k)
                PersonNameArray.append(dictValue)
                result_array.append(k+"~"+dictValue.strip())

    return  result_array


def getPageIndexinCat(category, continueFlag, gcmcontinue,list_pageIndex):

    list_local = list_pageIndex

    for cat in category:

        catName = cat[0]

        wikiFlag = cat[1]

        try:

            url = "https://<WIKI_FLAG>.wikipedia.org/w/api.php?action=query&generator=categorymembers&gcmlimit=500&gcmtitle=Category:"

            url = url.replace("<WIKI_FLAG>",wikiFlag)

            url= "".join([url,catName])       

            if(continueFlag == "TRUE"):

              url= "".join([url,"&continue=gcmcontinue||&gcmcontinue="])

              url= "".join([url,gcmcontinue])

           

            url = "".join([url,"&format=json"])

            jsonData = urlopen(url).read()

            json_object = json.loads(jsonData)

            try:

              for pageindex in json_object['query']['pages']:

                  list_local.append(pageindex)

              try:

                if json_object['continue'] != "":

                  getPageIndexinCat([[catName,wikiFlag]],"TRUE", json_object['continue']['gcmcontinue'],list_local)

              except:

                print("{} - Completed fetching page index for category : {}, (Cumulative) Records found: {}".format(datetime.now(), cat,len(list_local)))

                continue

              #print("{} - Fetching page index for category : {}, Records found: {}".format(datetime.now(), category,len(list_local)))

            except:

              print("{} - No record found - Fetching page index for category : {}, (Cumulative) Records found: {}".format(datetime.now(), cat,len(list_local)))

              continue

        except Exception as e:

              print("{} - ERROR - Check category input , category : {}, (Cumulative) Records found: {}".format(datetime.now(), cat,len(list_local)))

    return list_local

 
