
/*STEP1: Link Markit and Compustats using Ticker*/
/*STEP2: Find links for the remaining unmatched cases using Company names */
/*STEP3: Finzalizing the link*/


libname in "C:\Users\bhe2\Desktop\wrds\WRDS Data";
libname in1 "C:\Users\bhe2\Desktop\Yueran Ma\data";

ods html close;
ods html;

data funda;
	set in.funda_200101_201712; 
run;

data markit;
	set in1.markit;
run;

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


/* Step 2: Find links for the remaining unmatched cases using Company names */
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


/*link data with crsp*/

proc sql;
	create table link_m1_3
	as select a.* , b.ticker, b.permco, b.gvkey
	from link_m1_2 as a inner join link_table2 as b
	on a.gvkey = b.gvkey; 
quit;



