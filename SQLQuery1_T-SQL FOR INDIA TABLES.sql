USE [DATABASE ON CHILD POVERTY]
GO

CREATE SCHEMA india
GO
SELECT childid,round,careid,carerel,caredu,dadid,dadlive,dadedu,momid,momlive,momedu
INTO india.caregiver_and_biologicalparents_information
FROM dbo.india_constructed;

SELECT childid,round,bwght,numante,tetanus,delivery,bcg,measles,polio,dpt,hib
INTO india.Child_Birth_and_Immunisation
FROM dbo.india_constructed;

SELECT childid,round,preprim,enrol,agegr1,engrade,entype,hghgrade,levlread,levlwrit,literate
INTO india.Child_Education
FROM dbo.india_constructed;

SELECT childid,round,chsex,chethnic,agemon,marrcohab,marrcohab_age,birth
INTO india.Child_general_characteristics
FROM dbo.india_constructed;

SELECT childid,round,chhrel,chhealth,cladder
INTO india.Child_health_and_wellbeing
FROM dbo.india_constructed;

SELECT childid,clustid,commid,typesite,round,yc,childloc,region,deceased
INTO india.Child_identification
FROM dbo.india_constructed;

SELECT childid,round,chmightdie,chillness,chinjury,chhprob,chdisability
INTO india.Child_Illness_Injuries_disability
FROM dbo.india_constructed;

SELECT childid,round,chsmoke,chalcohol
INTO india.Child_Smoking_Drnking_habbits
FROM dbo.india_constructed;

SELECT childid,round,credit,foodsec
INTO india.Household_CreditandFood_Security
FROM dbo.india_constructed;

SELECT childid,headid,round,headsex,headedu,headrel
INTO india.Household_Head_charateristics
FROM dbo.india_constructed;

SELECT childid,round,ownlandhse,aniany,animilk,anidrau,anirumi,anispec
aniothr
INTO india.Household_LivestockandLand_ownership
FROM dbo.india_constructed;

SELECT childid,round,pds,nregs,nregs_work,nregs_allow,rajiv,sabla_yl,sabla
ikp,ikp_child
INTO india.Household_PublicProgrammes
FROM dbo.india_constructed;

SELECT childid,round,shcrime1,shcrime2,shcrime3,shcrime4,shcrime5,shcrime6,
shcrime7,shcrime8,shregul1,shregul2,shregul3,shregul4,shregul5,shecon1,shecon2,shecon3
shecon4,shecon5,shecon6,shecon7,shecon8,shecon9,shecon10,shecon11,shecon12,shecon13 shecon14, shenv1 
shenv2,shenv3,shenv4, shenv5, shenv6,shenv7,shenv8,shenv9,shhouse1,shhouse2,shhouse3,shfam1,shfam2,shfam3 
shfam4,shfam5, shfam6,shfam7,shfam8,shfam9 ,shfam10, shfam11,shfam12,shfam13,shfam18, shother
INTO india.Household_Shock
FROM dbo.india_constructed;


SELECT childid,round,male05,male612,male1317,male1860,male61,female05,female612,female1317,
female1860,female61,hhsize
INTO india.Household_size_and_composition
FROM dbo.india_constructed;

SELECT childid,round,wi,hq,sv,cd,elecq,toiletq,drwaterq,cookingq,deceased
INTO india.Household_WealthIndex_And_SubIndicies
FROM dbo.india_constructed;




















