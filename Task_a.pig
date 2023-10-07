faceInPage = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';

nationality_filter = FILTER faceInPage BY Nationality == 'Bulgaria';

STORE nationality_filter INTO 'Task_a_output';