faceInPage = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
associates = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/Associates.csv' USING PigStorage(',') as (FriendRel:int, PersonA_ID:int, PersonB_ID:int, DateofFriendship:int, Descrip:chararray);
accessLogs = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/AccessLogs.csv' USING PigStorage(',') as (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';
accessLogs = FILTER accessLogs BY NOT TypeOfAccess == 'TypeOfAccess';
associates = FILTER associates BY NOT Descrip == 'Desc';

switchedAssociates = FOREACH associates GENERATE FriendRel, PersonB_ID AS PersonA_ID, PersonA_ID AS PersonB_ID, DateofFriendship, Descrip;
combinedAssociates = UNION associates, switchedAssociates;
deduplicatedAssociates = DISTINCT combinedAssociates;

joinedData = JOIN deduplicatedAssociates BY (PersonA_ID, PersonB_ID) LEFT OUTER, accessLogs BY (ByWho, WhatPage);

filteredData = FILTER joinedData BY accessLogs::WhatPage IS NULL;

result = JOIN faceInPage BY ID, filteredData BY PersonA_ID;

formattedResult = FOREACH result GENERATE 
	filteredData::deduplicatedAssociates::PersonA_ID AS PersonA_ID,
    faceInPage::Name AS NameA,
	filteredData::deduplicatedAssociates::PersonB_ID AS PersonB_ID;

result2 = JOIN faceInPage BY ID, formattedResult BY PersonB_ID;

formattedResult2 = FOREACH result2 GENERATE 
	formattedResult::PersonA_ID AS PersonA_ID,
    formattedResult::NameA AS NameA,
	formattedResult::PersonB_ID AS PersonB_ID,
	faceInPage::Name AS NameB;
	
STORE formattedResult2 INTO 'Task_f_output';

