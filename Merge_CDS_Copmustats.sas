libname in "C:\Users\bhe2\Desktop\wrds\WRDS Data";

ods html close;
ods html;

data funda;
	set in.funda_201301_201808; 
run;


data markit;
	set in.markit_201301_201808;
run;


proc contents data=funda;
run;


proc contents data=markit;
run;

/**/
/*1. no ticker*/
/*2. tickers are "111A" kind. but company name can still be used to identify*/
/*3. tickers end with "." or AA.1 to mark as old stock*/


proc freq data=funda;                                                         *missing 32;
	tables tic / nocum nopercent out= missing_tic ( where=(Tic = ""));
run;

proc freq data=funda nlevels;
	tables tic  conm  fyear / nopct nocum  out = levels_funda;
run;



                                                                                *missing 0;
proc freq data=markit;
	tables ticker / nocum nopercent out= missing_tickers( where=(Ticker = ""));
run;


proc freq data= markit nlevels;
	tables ticker  shortname  date / nopct nocum out=levels_markit;
run;



*want to examine company names first;

proc sort data = funda out=funda_sort nodupkey;   
	by tic conm;
run;

proc sort data = markit out=markit_sort nodupkey;
	by ticker shortname redcode;
run;


/*data cleaning, CDS ticker;*/

data markit_sort;
	set markit_sort;
	ticker1 = scan(ticker,1,"-");
run;


data markit1 (keep = ticker ticker1 shortname redcode);
	set markit_sort;
run;


data funda1 ( keep= tic conm);
	set funda_sort;
run;


proc sql;                                            *601 successfully merged;
	create table inner_join as
	select * from markit1 a
		inner join 
			funda1 b
		on a.ticker1 = b.tic;
quit;

proc sql;
	create table full_join as
	select * from markit1 a 
		full join
			funda1 b
		on a.ticker1 = b.tic;
quit;





* Link Markit CDS (Ticker,REDcode) and Compustats(crisp);


data _MKT1;
	set markit;
	ticker1 = scan(ticker,1,"-");
run;

proc sort data = _MKT1 out=_MKT2 (keep = ticker ticker1 shortname redcode) nodupkey;
	by ticker;
run;


proc sort data = funda out=_FUN2 (keep = tic conm) nodupkey;
	by tic;
run;

/*Step One, match with truncated CDS ticker and Cusip Ticker*/
proc sql;                                                             *591;
	create table _link1_1
	as select *
	from _MKT2 as a, _FUN2 as b
	where a. ticker1= b.tic
	order by shortname, conm;
quit;

/*data _link1_2;*/
/*	set  _link1_1;*/
/*	by shortname conm;*/
/*	if ticker ^= tic;*/
/*	name_dist = min(spedis(shortname,conm),spedis(conm,shortname));*/
/*run;*/
/**/

proc sql;
	create table _nomatch1
	as select distinct a.*
	from  _MKT1 (keep = ticker ticker1 shortname) as a
	where a.ticker1 NOT in (select distinct tic from _link1_1);
quit;
