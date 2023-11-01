import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import pandas as pd
import requests
import json
from dagster import (
    MetadataValue,
    Output,
    asset,
)

@asset
def Freeway_Bureau():
    url = "https://tisvcloud.freeway.gov.tw/history/motc20/LiveTraffic.xml"
    response = requests.get(url)
    response.raise_for_status()
    xml_data = response.text
    root = ET.fromstring(xml_data)

    results = []
    columns = ['SectionID', 'TravelTime', 'TravelSpeed', 'CongestionLevelID', 'CongestionLevel','HasHistorical', 'HasVD', 'HasAVI', 'HasETAG', 'HasGVP', 'HasCVP', 'HasOthers']
    for live_traffic in root.findall('*/{http://traffic.transportdata.tw/standard/traffic/schema/}LiveTraffic'):
        SectionID = live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}SectionID').text
        TravelTime= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelTime').text
        TravelSpeed= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelSpeed').text
        CongestionLevelID= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevelID').text
        CongestionLevel= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevel').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel, sep='\t')
        for DataSources in root.findall('*//{http://traffic.transportdata.tw/standard/traffic/schema/}DataSources'):
            HasHistorical = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasHistorical').text
            HasVD = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasVD').text
            HasAVI = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasAVI').text
            HasETAG = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasETAG').text
            HasGVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasGVP').text
            HasCVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasCVP').text
            HasOthers = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasOthers').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel,HasHistorical,HasVD,HasAVI,HasETAG,HasGVP,HasCVP,HasOthers, sep='\t')
        results.append([SectionID, TravelTime, TravelSpeed, CongestionLevelID, CongestionLevel, HasHistorical, HasVD, HasAVI, HasETAG, HasGVP, HasCVP, HasOthers])

    df = pd.DataFrame(results,columns=columns)

    return Output(
        value=df,
        metadata={
           "num_records": len(df),
           "preview": MetadataValue.md(df.head().to_markdown()),
       },
    )

@asset()
def Directorate_of_General_Highways():

    url = "https://thbapp.thb.gov.tw/opendata/section/livetrafficdata/LiveTrafficList.xml"
    response = requests.get(url)
    response.raise_for_status()
    xml_data = response.text
    root = ET.fromstring(xml_data)

    results2 = []
    columns = ['SectionID', 'TravelTime', 'TravelSpeed', 'CongestionLevelID', 'CongestionLevel','HasHistorical', 'HasVD', 'HasAVI', 'HasETAG', 'HasGVP', 'HasCVP', 'HasOthers']
    for live_traffic in root.findall('*/{http://traffic.transportdata.tw/standard/traffic/schema/}LiveTraffic'):
        SectionID = live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}SectionID').text
        TravelTime= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelTime').text
        TravelSpeed= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelSpeed').text
        CongestionLevelID= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevelID').text
        CongestionLevel= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevel').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel, sep='\t')
        for DataSources in root.findall('*//{http://traffic.transportdata.tw/standard/traffic/schema/}DataSources'):
            HasHistorical = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasHistorical').text
            HasVD = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasVD').text
            HasAVI = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasAVI').text
            HasETAG = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasETAG').text
            HasGVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasGVP').text
            HasCVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasCVP').text
            HasOthers = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasOthers').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel,HasHistorical,HasVD,HasAVI,HasETAG,HasGVP,HasCVP,HasOthers, sep='\t')
        results2.append([SectionID, TravelTime, TravelSpeed, CongestionLevelID, CongestionLevel, HasHistorical, HasVD, HasAVI, HasETAG, HasGVP, HasCVP, HasOthers])

    df = pd.DataFrame(results2,columns=columns)

    return Output(
        value=df,
        metadata={
           "num_records": len(df),
           "preview": MetadataValue.md(df.head().to_markdown()),
       },
    )


@asset()
def Taipei_Traffic():

    url = "https://thbapp.thb.gov.tw/opendata/section/livetrafficdata/LiveTrafficList.xml"
    response = requests.get(url)
    response.raise_for_status()
    xml_data = response.text
    root = ET.fromstring(xml_data)

    results3 = []
    columns = ['SectionID', 'TravelTime', 'TravelSpeed', 'CongestionLevelID', 'CongestionLevel','HasHistorical', 'HasVD', 'HasAVI', 'HasETAG', 'HasGVP', 'HasCVP', 'HasOthers']
    for live_traffic in root.findall('*/{http://traffic.transportdata.tw/standard/traffic/schema/}LiveTraffic'):
        SectionID = live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}SectionID').text
        TravelTime= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelTime').text
        TravelSpeed= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelSpeed').text
        CongestionLevelID= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevelID').text
        CongestionLevel= live_traffic.find('{http://traffic.transportdata.tw/standard/traffic/schema/}CongestionLevel').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel, sep='\t')
        for DataSources in root.findall('*//{http://traffic.transportdata.tw/standard/traffic/schema/}DataSources'):
            HasHistorical = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasHistorical').text
            HasVD = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasVD').text
            HasAVI = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasAVI').text
            HasETAG = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasETAG').text
            HasGVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasGVP').text
            HasCVP = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasCVP').text
            HasOthers = DataSources.find('{http://traffic.transportdata.tw/standard/traffic/schema/}HasOthers').text
        #print(SectionID,TravelTime,TravelSpeed,CongestionLevelID,CongestionLevel,HasHistorical,HasVD,HasAVI,HasETAG,HasGVP,HasCVP,HasOthers, sep='\t')
        results3.append([SectionID, TravelTime, TravelSpeed, CongestionLevelID, CongestionLevel, HasHistorical, HasVD, HasAVI, HasETAG, HasGVP, HasCVP, HasOthers])

    df = pd.DataFrame(results3,columns=columns)

    return Output(
        value=df,
        metadata={
           "num_records": len(df),
           "preview": MetadataValue.md(df.head().to_markdown()),
       },
    )  


@asset()
def Weather_Data():
    url = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/F-C0032-001?Authorization=CWA-3A61AFA8-BF84-4F8B-B56B-5C4608710D53&format=XML&locationName=&elementName=" 
    response = requests.get(url)
    response.raise_for_status()
    xml_data = response.text
    root = ET.fromstring(xml_data)

    weather_data = []
    columns = ['locationName', 'elementName', 'startTime', 'endTime', 'parameterName', 'parameterUnit']
    for location in root.findall(".//location"):
        location_name = location.findtext("locationName")
        for weather_element in location.findall(".//weatherElement"):
            element_name = weather_element.findtext("elementName")
            for time_element in weather_element.findall(".//time"):
                start_time = time_element.findtext("startTime")
                end_time = time_element.findtext("endTime")
                for parameter in time_element.findall("parameter"):
                    parameter_name = parameter.findtext("parameterName")
                    #parameter_value = parameter.findtext("parameterValue")
                    parameter_unit = parameter.findtext("parameterUnit")
                    weather_data.append([location_name, element_name, start_time, end_time, parameter_name, parameter_unit])

    df = pd.DataFrame(weather_data,columns=columns)

    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )

@asset()
def teamE_GPS():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset()
def teamG_GPS():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset()
def teamT_GPS():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset
def OpenData():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)
        
@asset
def selected_area():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset
def digital_map():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset
def getDayData():
    url = "https://iiitraffic.com/getDayData" 
    response = requests.get(url)

@asset
def getTainanRealtime():
    url = "https://iiitraffic.com/getTainanRealtime" 
    response = requests.get(url)

@asset
def OIDC_Authentication():
    url = "https://iiitraffic.com/oidc_callback" 
    response = requests.get(url)