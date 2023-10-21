faceInPage = LOAD 'hdfs://localhost:9000/project2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
accessLogs = LOAD 'hdfs://localhost:9000/project2/AccessLogs.csv' USING PigStorage(',') as (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';
accessLogs = FILTER accessLogs BY NOT TypeOfAccess == 'TypeOfAccess';

groupedAccessLogs = GROUP accessLogs BY ByWho;

minAccessLogs = FOREACH groupedAccessLogs {
    orderedLogs = ORDER accessLogs BY AccessTime;
    firstLog = LIMIT orderedLogs 1;
    GENERATE FLATTEN(firstLog);
}

combinedFields = JOIN faceInPage BY ID LEFT OUTER, minAccessLogs BY ByWho;

outdatedPages = FILTER combinedFields BY (AccessTime IS NULL) OR (AccessTime/(60*24)) > 90;

formattedResult = FOREACH outdatedPages GENERATE 
	faceInPage::ID AS ID,
	faceInPage::Name AS Name;
	
STORE formattedResult INTO 'hdfs://localhost:9000/project2/Task_g_output';