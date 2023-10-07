faceInPage = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
accessLogs = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/AccessLogs.csv' USING PigStorage(',') as (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';
accessLogs = FILTER accessLogs BY NOT TypeOfAccess == 'TypeOfAccess';

totalPageVisits = GROUP accessLogs BY ByWho;
countedTotalVisits = FOREACH totalPageVisits GENERATE group AS ByWho, COUNT(accessLogs) AS totalVisits;

uniquePageVisits = GROUP accessLogs BY (ByWho, WhatPage);
countUniquePageVisits = FOREACH uniquePageVisits GENERATE group.ByWho AS ByWho, group.WhatPage AS WhatPage, COUNT(accessLogs) AS totalUniqueVisits;
uniqueCountGroup = Group countUniquePageVisits BY ByWho;
uniqueCount = FOREACH uniqueCountGroup GENERATE group AS ByWho, COUNT(countUniquePageVisits) AS EntryCount;

result = JOIN faceInPage BY ID LEFT OUTER, countedTotalVisits BY ByWho;
result2 = JOIN result BY ID LEFT OUTER, uniqueCount BY ByWho;

formattedResult = FOREACH result2 GENERATE 
    faceInPage::Name AS Name, 
    (countedTotalVisits::totalVisits is not null ? countedTotalVisits::totalVisits : 0) AS totalVisits,
	(uniqueCount::EntryCount is not null ? uniqueCount::EntryCount : 0) AS totalUniqueVisits;

STORE formattedResult INTO 'Task_e_output';