faceInPage = LOAD 'hdfs://localhost:9000/project2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';

nationality_group = Group faceInPage BY Nationality;

nationality_count = FOREACH nationality_group GENERATE group AS Nationality, COUNT(faceInPage) AS EntryCount;

STORE nationality_count INTO 'hdfs://localhost:9000/project2/Task_c_output';