libname in "C:\Users\bhe2\Desktop\wrds\WRDS Data";
libname in1 "C:\Users\bhe2\Desktop\Yueran Ma\data";

*load in data;
data funda;
	set in.funda_200101_201712; 
run;

data markit;
	set in.markit;
run;

data crsp;
	set in.crsp_200101_201712;
run;

PROC IMPORT OUT= WORK.ccm_compustat
            DATAFILE= "C:\Users\bhe2\Desktop\wrds\WRDS Data\ccm_compusta
t.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



PROC IMPORT OUT= WORK.JennieLink 
            DATAFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\link_CRSP_Markit.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;



proc sort data=ccm_compustat out= _link_table nodupkey; by gvkey datadate tic conm;run; 

data _link_table1;
	set _link_table;
	if  '01Jan2001' <= datadate<= '31Dec2017';
run;

proc sort data= _link_table1 out=link_table nodupkey; by gvkey permco tic conm;run;


data jennielink;
	set jennielink;
	permco1= permco*1;
run;

/*Link with Jennie data*/
data jennielink1;
	set jennielink;
	if permco1 ^= .;
run;

data _link_table;
	set link_table2;
	if permco ^=.;
run;
proc sql;
	create table link_m2_1
	as select a.permco1, a.shortname0, b.gvkey, b.tic, b.conm, b.permco
	from jennielink as a inner join _link_table as b
	on a.permco1 = b.permco; 
quit;



