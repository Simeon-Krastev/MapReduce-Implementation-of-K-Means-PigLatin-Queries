faceInPage = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/FaceInPage.csv' USING PigStorage(',') as (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
associates = LOAD 'C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/Associates.csv' USING PigStorage(',') as (FriendRel:int, PersonA_ID:int, PersonB_ID:int, DateofFriendship:int, Descrip:chararray);

faceInPage = FILTER faceInPage BY NOT Nationality == 'Nationality';
associates = FILTER associates BY NOT Descrip == 'Desc';

switchedAssociates = FOREACH associates GENERATE FriendRel, PersonB_ID AS PersonA_ID, PersonA_ID AS PersonB_ID, DateofFriendship, Descrip;

combinedAssociates = UNION associates, switchedAssociates;

deduplicatedAssociates = DISTINCT combinedAssociates;

relationshipCounts = GROUP deduplicatedAssociates BY PersonA_ID;
countedRelationships = FOREACH relationshipCounts GENERATE group AS PersonA_ID, COUNT(deduplicatedAssociates) AS RelationshipCount;

result = JOIN faceInPage BY ID LEFT OUTER, countedRelationships BY PersonA_ID;

formattedResult = FOREACH result GENERATE 
    faceInPage::Name AS Name, 
    (countedRelationships::RelationshipCount is not null ? countedRelationships::RelationshipCount : 0) AS RelationshipCount;

grouped = GROUP formattedResult ALL;

average = FOREACH grouped GENERATE AVG(formattedResult.RelationshipCount) AS average;

popular = FILTER formattedResult BY (RelationshipCount > average.average);

averageRelation = FOREACH average GENERATE 'Average' AS Name, average AS RelationshipCount;

final_result = UNION popular, averageRelation;

STORE final_result INTO 'Task_h_output';
