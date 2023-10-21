faceInPage = LOAD 'hdfs://localhost:9000/project2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
accessLogs = LOAD 'hdfs://localhost:9000/project2/AccessLogs.csv' USING PigStorage(',') as (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';
accessLogs = FILTER accessLogs BY NOT TypeOfAccess == 'TypeOfAccess';

accessCounts = FOREACH (GROUP accessLogs BY WhatPage) GENERATE group AS PageId, COUNT(accessLogs) AS AccessCount;

rankedPages = RANK accessCounts BY AccessCount DESC;

top10Pages = LIMIT rankedPages 10;

result = JOIN top10Pages BY PageId, faceInPage BY ID;

formattedResult = FOREACH result GENERATE faceInPage::ID AS Id, faceInPage::Name AS Name, faceInPage::Nationality AS Nationality;

STORE formattedResult INTO 'hdfs://localhost:9000/project2/Task_b_output';