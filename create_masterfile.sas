libname in1"C:\Users\bhe2\Desktop\Yueran Ma\data";


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



*Mannually checked for subsidiaries and flagged suspicious entry
read in, get permno code for bianca_unmatch;

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


*Link crsp data to Bianca_Unmatch;


data _unmatch_b0;
	set _unmatch_b additional;
run;

proc sql;
	create table _unmatch_b1
	as select a.* , b.ticker, b.comnam, b.permco, b.gvkey
	from _unmatch_b0 as a left join cclink_clean as b
	on a.gvkey = b.gvkey; 
quit;

proc sort data = _unmatch_b1 out= _UNMATCH_B2 ; by ticker;run;

data Merged;
	set MATCH _UNMATCH_B2 _unmatch_j;
run;

proc sort data = merged; by gvkey; run;


/*merge the link with markit to check the spread*/
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
* Merge the file with Markit data;


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
