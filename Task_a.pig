faceInPage = LOAD 'hdfs://localhost:9000/project2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';

nationality_filter = FILTER faceInPage BY Nationality == 'Bulgaria';

STORE nationality_filter INTO 'hdfs://localhost:9000/project2/Task_a_output';