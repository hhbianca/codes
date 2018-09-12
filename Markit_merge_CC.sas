/*STEP1: LINK METHOD1, using Markit ticker and shortname to link Markit and Funda;*/
/*STEP2: LINK METHOD2, using Jennie (permco and shortname) to link Markit and CRSP;*/
/*STEP3: USE FUNDA and CRSP LINK to Complete the link;*/
/*STEP4: Merge the Links, Check the Unmatches;*/
/*STEP5: Mannually Flag the subsidiaries and Create the Masterfile;*/
/*STEP6: Merge with Markit;*/

libname in "C:\Users\bhe2\Desktop\wrds\WRDS Data";
libname in1 "C:\Users\bhe2\Desktop\Yueran Ma\data";
data funda;
	set in.funda_200101_201712; 
run;
data markit;
	set in1.markit;
run;
data funda;
	set in.funda_200101_201712; 
run;
data crsp;
	set in.crsp_200101_201712;
run;
PROC IMPORT OUT= WORK.ccm_compustat
            DATAFILE= "C:\Users\bhe2\Desktop\wrds\WRDS Data\ccm_compustat.csv" 
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

/*STEP1: LINK METHOD1, using Markit ticker and shortname to link Markit and Funda;*/

*Data Examination;
proc sort data=markit out=markit_s nodupkey; by REDcode shortname;run;            *total 2137 ->2246 CDS-shortname;

data duplicate_redcode;                                                           *Shortname spelling is the one to blame. 94 -> 102 of them.;
	set markit_s;
	by redcode;
	if first.redcode + last.redcode <2;
run;
	

proc sort data=markit out=_MKT1 (keep=date ticker redcode shortname);
  by redcode ticker date;
run;



/*STEP1 Link using Ticker */

/* Create first and last 'start dates' for Markit link */
proc sql;
  create table _MKT2
  as select *, min(date) as fdate, max(date) as ldate
  from _MKT1
  group by redcode, ticker
  order by redcode, ticker, date;
quit;
 
/* Label date range variables and keep only most recent company name for Markit link */					*CDS 2094 companies;
data _MKT2;
  set _MKT2;
  by redcode ticker;
  if last.ticker;
  label fdate="First Start date of ticker record";
  label ldate="Last Start date of ticker record";
  format fdate ldate date9.;
  drop date;
run;

/*Strip markit ticker "change ABX-fin to ABX"*/
data _MKT3;
	set _MKT2;
	ticker1 = scan(ticker,1,"-");
run;

/* CRSP: Get all PERMNO-NCUSIP combinations */
proc sort data=funda out=_FUN1 (keep=gvkey tic conm datadate);
  where not missing(tic);
  by gvkey tic conm;
run;
 
/* Arrange effective dates for Markit link */
proc sql;
  create table _FUN2
  as select gvkey,tic,conm,min(datadate)as namedt,max(datadate) as nameenddt
  from _FUN1
  group by gvkey, tic
  order by gvkey, tic, NAMEDT;
quit;
 
/* Label date range variables and keep only most recent company name */
data _FUN2;
  set _FUN2;
  by gvkey tic;
  if last.tic;
  label namedt="Start date of ticker record";
  label nameenddt="End date of ticker record";
  format namedt nameenddt date9.;
run;
 


/* Create Markit Link Table */
/* Markit date ranges are only used in scoring as Markit are not reused for
    different companies overtime */
																									*100% ticker mrerge 729;
proc sql;
  create table _LINK1_0                                                                                                                  
  as select *
  from _MKT2 as a, _FUN2 as b
  where a.ticker = b.tic
  order by redcode, gvkey, ldate;
quit;
   
	  																						        *strip Ticker can merge 1043;
proc sql;
  create table _LINK1_2                                                                                                                  
  as select *
  from _MKT3 as a, _FUN2 as b
  where a.ticker1 = b.tic
  order by redcode, gvkey, ldate;
quit;
 
/* Score links using Markit date range and company name spelling distance */
/* Idea: date ranges the same ticker was used in Funda and Markit should intersect */    	      *check name and time merge 978;
data _LINK1_3;
  set _LINK1_2;
  by redcode gvkey;
  if last.gvkey; /* Keep link with most recent company name */
  name_dist = min(spedis(shortname,conm),spedis(shortname,conm));
  if (not ((ldate < namedt) or (fdate > nameenddt))) and name_dist < 30 then SCORE = 0;
    else if (not ((ldate < namedt) or (fdate > nameenddt))) then score = 1;
    else if name_dist < 30 then SCORE = 2;
      else SCORE = 3;
  keep redcode gvkey ticker tic shortname conm;
  if score <3;
run;


/* Find links for the remaining unmatched cases using Company names */
/* Identify remaining unmatched cases */															  * Non matched 1109 left;

proc sql;
  create table _NOMATCH1
  as select distinct a.*
  from _MKT1 (keep=redcode) as a 																
  where a.redcode NOT in (select distinct redcode from _LINK1_3)
  order by a.redcode;
quit;
 
 
/* Add Markit identifying information & drop tickers that have ? or ZZZZ */;
proc sql;
  create table _NOMATCH2
  as select b.redcode, b.shortname, b.ticker, b.date
  from _NOMATCH1 as a, Markit as b
  where a.redcode = b.redcode and not (missing(b.shortname))
   and ticker not like '%?%' and ticker not like '%ZZZZ%'
  order by redcode, shortname, date;
quit; 
 
/* Create first and last 'start dates' for Company */
proc sql;
  create table _NOMATCH3
  as select *, min(date) as fdate, max(date) as ldate
  from _NOMATCH2
  group by redcode, shortname
  order by redcode, shortname, date;
quit;
                          
/* Label date range variables and keep only most recent company name */                         
data _NOMATCH3;
  set _NOMATCH3;
  by redcode shortname;
  if last.shortname;
  label fdate="First Start date of record";
  label ldate="Last Start date of record";
  format fdate ldate date9.;
  drop date;
run;

 
/* Get entire list of Funda stocks with Markit information */
/* Give Funda's Ticker precedence over Funda Standardized Ticker */
proc sql;
create table _FUN1
as select conm, tic, gvkey, datadate
from FUNDA
order by gvkey, conm, datadate;
run;
 
/* Arrange effective dates for link by Company Name */
proc sql;
  create table _FUN2
  as select gvkey,conm,tic,
              min(datadate)as namedt,max(datadate) as nameenddt
  from _FUN1
  where not missing(tic)
  group by gvkey, tic
  order by gvkey, tic, namedt;
quit;
 
/* Label date range variables and keep only most recent company name */
data _FUN2;
  set _FUN2;
  by gvkey conm;
  if  last.conm;
  label namedt="Start date of company record";
  label nameenddt="End date of company record";
  format namedt nameenddt date9.;
run;
 
/* Merge remaining unmatched cases using Company */
/* Note: Use ticker date ranges as Company are reused overtime */                       *11 matched by company name;
proc sql;
  create table _LINK2_1
  as select a.redcode,a.ticker, b.gvkey, a.shortname, b.conm, b.tic, a.ldate
  from _NOMATCH3 as a, _FUN2 as b
  where strip(a.shortname) = strip(b.conm) and
     (ldate >= namedt) and (fdate <= nameenddt)
  order by redcode, shortname, conm, ldate;
quit;
 

/*very good match so far are _LINK1_3 and _LINK2_1*/
/*The still NO Matched ones are tried by Company names. However, INC, LLC endings might means huge difference for firms*/
proc sql;
  create table _NOMATCH4
  as select distinct a.*
  from _NOMATCH3  as a 																
  where a.redcode NOT in (select distinct redcode from _LINK2_1)
  order by a.redcode;
quit;


%let vals = INC|LLC|CORPORATION|CORP|CO|COMPANY|LTD;

data _NOMATCH4;
set _NOMATCH4;
name_markit = shortname;
name_markit = upcase(name_markit);
regex = prxparse("s/\b(&vals.)\b//i"); /* /b signifies a word boundary, so it will remove the whole words only */
call prxchange(regex,-1,name_markit);
drop regex;
run;


data _fun2;
	set _fun2;
name_funda = conm;
name_funda = upcase(name_funda);
regex = prxparse("s/\b(&vals.)\b//i"); /* /b signifies a word boundary, so it will remove the whole words only */
call prxchange(regex,-1,name_funda);
drop regex;
run;


proc sql;
  create table _LINK3_1
  as select a.redcode,a.ticker, b.gvkey, a.shortname, b.conm, b.tic, a.ldate, a.fdate, b.namedt, b.nameenddt, a.name_markit, b.name_funda
  from _NOMATCH4 as a, _FUN2 as b
  where a.name_markit = b.name_funda and
     (ldate >= namedt) and (fdate <= nameenddt)
  order by redcode, shortname, conm, ldate;
quit;


proc sort data = _LINK3_1; by redcode gvkey; run;



data _LINK3_2;																						*201 fuzzy match;																		
  set _LINK3_1;
  by redcode gvkey;
  if last.gvkey; /* Keep link with most recent company name */                                 
  ticker1 = scan(ticker,1,"-");
  name_dist = min(spedis(ticker1,tic),spedis(tic,ticker1));
  if (not ((ldate < namedt) or (fdate > nameenddt))) and name_dist < 10 then SCORE = 0;
    else if (not ((ldate < namedt) or (fdate > nameenddt))) then score = 1;
    else if name_dist < 10 then SCORE = 2;
      else SCORE = 3;
  if score <3;
  keep redcode gvkey shortname conm ticker tic;

run;


*Step3: Finalizing Links and Scores...; 

/* Step 3: Add Company Name links to Funda links      */
/* Create Labels for CCLINK dataset and variables        */
/* Create final link table */

data LINK_m1_1;
	set _LINK1_3 _LINK2_1;
run;

data LINK_m1;
	set LINK_m1_1;
	keep redcode gvkey shortname conm ticker tic;
run;

data LINK_m1_2;
  set _LINK1_3 _LINK2_1 _LINK3_2;
run;

data LINK_m1_2;
	set LINK_m1_2;
	keep redcode gvkey shortname conm ticker tic;
run;



/*STEP2: LINK METHOD2, using Jennie (permco and shortname) to link Markit and CRSP;*/




data jennielink;
	set jennielink;
	permco1= permco*1;
run;
data jennielink1;
	set jennielink;
	if permco1 ^= .;
run;

data _link_table;
	set link_table2;
	if permco ^=.;
run;



/*STEP3: USE FUNDA and CRSP LINK to Complete the link;*/

proc sort data=ccm_compustat out= _link_table nodupkey; by gvkey datadate tic conm;run; 

data _link_table1;
	set _link_table;
	if  '01Jan2001' <= datadate<= '31Dec2017';
run;

proc sort data= _link_table1 out=link_table nodupkey; by gvkey permco tic conm;run;

/*link Bianca data with CRSP*/
proc sql;
	create table link_m1_3
	as select a.* , b.ticker, b.permco, b.gvkey
	from link_m1_2 as a inner join link_table2 as b
	on a.gvkey = b.gvkey; 
quit;

/*link Jennie data with CRSP and COMPUSTAT*/
proc sql;
	create table link_m2_1
	as select a.permco1, a.shortname0, b.gvkey, b.tic, b.conm, b.permco
	from jennielink as a inner join _link_table as b
	on a.permco1 = b.permco; 
quit;
/*STEP4: Merge the Links, Check the Unmatches;*/



data markit;
	set in1.markit;
run;

proc sql;
	create table MATCH as
	select a.redcode, a.shortname, a.ticker,a.tic,a.gvkey, a.conm, b.gvkey, b.permco, b.shortname0
	from link_m1_3 as a inner join link_m2_1 as b
	on a.permco = b.permco and a.shortname = b.shortname0 and a.gvkey = b.gvkey;
quit;  

proc sql;
  create table _NOMATCH_Bianca
  as select a.*
  from link_m1_2 as a 																
  where a.gvkey NOT in (select distinct gvkey from match)
  order by a.gvkey;
quit;


proc sql;
	create table _NOMATCH_Jennie
	as select a.*
	from link_m2_1 as a
	where a.gvkey not in (select distinct gvkey from match)
	order by a.gvkey;
quit;

data _mkt2;
	set _mkt2;
	ticker_M = ticker;
run;


proc sql;
	create table _NOMATCH_Jennie1
	as select a.*, b.ticker_m, b.shortname, b.redcode
	from _NOMATCH_jennie as a left join _mkt2 as b
	on a.shortname0 = b.shortname;
quit;


data _NOMATCH_Jennie1 (keep = redcode shortname gvkey tic conm permco comnam ticker);
 	set _NOMATCH_Jennie1(drop = ticker);
	rename ticker_m = ticker;
run;




PROC EXPORT DATA= WORK._NOMATCH_Bianca
            OUTFILE= "C:\Users\bhe2\Desktop\Suspicious1.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="Bianca_unmatch"; 
RUN;
PROC EXPORT DATA= WORK._NOMATCH_Jennie1
            OUTFILE= "C:\Users\bhe2\Desktop\Suspicious1.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="Jennie_unmatch"; 
RUN;

PROC EXPORT DATA= WORK.MATCH
            OUTFILE= "C:\Users\bhe2\Desktop\Suspicious1.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="Match"; 
RUN;





/*STEP5: Mannually Flag the subsidiaries and Create the Masterfile;*/
PROC IMPORT OUT= WORK._Unmatch_B 
            DATAFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\Suspicious.xls" 
            DBMS=EXCEL REPLACE;
     RANGE="Bianca_unmatch$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC IMPORT OUT= WORK._UNMATCH_J
            DATAFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\Suspicious.xls" 
            DBMS=EXCEL REPLACE;
     RANGE="Jennie_unmatch$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

*newly added data to adjust for LEH, BEAR, and MS;
PROC IMPORT OUT= WORK.additional
            DATAFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\Suspicious.xls" 
            DBMS=EXCEL REPLACE;
     RANGE="additional$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
proc sort data = markit out = markit_s nodupkey; by ticker redcode;run;

proc sql;
	create table markit_master as
	select a.permco, a.gvkey, a.redcode, a.ticker,a.shortname, a.susp_flag, a.subsi_flag, b.redcode, b.shortname,b.spread5y
	from merged as a left join markit_s as b
	on a.redcode=b.redcode;
quit;

proc sort data =markit_master;by ticker redcode shortname;run;

PROC EXPORT DATA= WORK.markit_master
            OUTFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\markit_master4.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="result"; 
RUN;

proc sort data = merged; by ticker redcode; run;


/*STEP6: Merge with Markit;*/

PROC IMPORT OUT= WORK.MARKIT_MASTER 
            DATAFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\markit_maste
r4.xls" 
            DBMS=EXCEL REPLACE;
     RANGE="result"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data markit_master;
	set markit_master;
	if susp_flag = . then susp_flag = "0";
	if subsi_flag =. then subsi_flag = "0";
run;

proc sort data =markit; by redcode ticker shortname date;run;
proc sort data = markit_master; by redcode ticker shortname;run;


data markit_redcode markit_noredcode;
	set markit;
	if redcode = "" then output markit_noredcode;
	else output markit_redcode;
run;

data master_file1;
	merge markit_redcode (in = a)
		 markit_master (in = b);
		 by redcode;
	if a;
run;

proc sort data = markit_noredcode; by shortname; run;
proc sort data = markit_master; by shortname;run;

data master_file2;
	merge markit_noredcode (in=a)
		markit_master (in = b);
		by shortname;
	if a;
run;

data master_file;
	set master_file1 master_file2;
run;


data in1.markit_masterfile;
	set MASTER_FILE;
run;



PROC EXPORT DATA= WORK.MASTER_FILE 
            OUTFILE= "C:\Users\bhe2\Desktop\Yueran Ma\data\markit_master
file.dta" 
            DBMS=STATA REPLACE;
RUN;
