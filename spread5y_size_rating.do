*STEP1: Create ranking using Yuran's code
*STEP2: Merge with Markit_size datasets
*STEP3: Plot the graphs

use "C:\Users\bhe2\Dropbox (Chicago Booth)\FinancialFirms\data\compustat\sprating_firm_7516.dta", clear

gen ym = mofd(datadate)
format ym %tm

gen cusip6 = substr(cusip, 1, 6)
duplicates drop cusip6 ym, force

tempfile fratings
save "`fratings'"

use "C:\Users\bhe2\Dropbox (Chicago Booth)\FinancialFirms\data\compustat\compustat\sprating_firm_7516.dta", clear
*Firm ratings
gen cusip6 = substr(cusip, 1, 6)
* Bianca added this...
gen yq = qofd(datadate)     

gen ym = mofd(dofq(yq)) - 12  
merge m:1 cusip6 ym using "`fratings'", keep(1 3) keepusing(sp*rm)
tab _merge
drop _merge
replace ym = mofd(dofq(yq))

gen fratings = .
replace fratings = 1 if splticrm=="AAA"  
replace fratings = 2 if splticrm=="AA+"  
replace fratings = 3 if splticrm=="AA"  
replace fratings = 4 if splticrm=="AA-"  
replace fratings = 5 if splticrm=="A+"  
replace fratings = 6 if splticrm=="A"  
replace fratings = 7 if splticrm=="A-"  
replace fratings = 8 if splticrm=="BBB+" 
replace fratings = 9 if splticrm=="BBB" 
replace fratings = 10 if splticrm=="BBB-"  
replace fratings = 11 if splticrm=="BB+"  
replace fratings = 12 if splticrm=="BB"  
replace fratings = 13 if splticrm=="BB-"  
replace fratings = 14 if splticrm=="B+"  
replace fratings = 15 if splticrm=="B"  
replace fratings = 16 if splticrm=="B-"  
replace fratings = 17 if splticrm=="CCC+"  
replace fratings = 18 if splticrm=="CCC"  
replace fratings = 19 if splticrm=="CCC-"  
replace fratings = 20 if splticrm=="CC"  
replace fratings = 21 if splticrm=="C"  
replace fratings = 22 if splticrm=="D" | splticrm=="SD" 
//replace fratings = 23 if splticrm=="NR"  
replace fratings = 99 if splticrm==""

gen fcatg = 1 if fratings==1  /*AAA*/
replace fcatg = 2 if fratings>=2 & fratings<=4 /*AA*/
replace fcatg = 3 if fratings>=5 & fratings<=7 /*A*/
replace fcatg = 4 if fratings>=8 & fratings<=10 /*BBB*/
replace fcatg = 5 if fratings>=11 & fratings<=13 /*BB*/
replace fcatg = 6 if fratings>=14 & fratings<=16 /*B*/
replace fcatg = 7 if fratings>=17 & fratings<=22 /*C&D*/
//replace fcatg = 8 if fratings>=20 & fratings<=22 /*C&D*/
//replace catg = 9 if fratings==23 /*NR*/
replace fcatg = 99 if fcatg==.

label define ratings  1 "AAA" 2 "AA+"  3 "AA" 4 "AA-"  5 "A+" 6 "A" 7 "A-" 8 "BBB+" 9 "BBB" 10 "BBB-" 11 "BB+" 12 "BB" 13 "BB-" ///
14 "B+" 15 "B" 16 "B-" 17 "CCC+" 18 "CCC" 19 "CCC-" 20 "CC" 21 "C" 22 "D" 99 "NR", replace 
label define catg 1 "AAA" 2 "AA" 3 "A" 4 "BBB" 5 "BB" 6 "B" 7 "C&D" 99 "NR"
label val fratings ratings
label val fcatg catg

gen qtr = quarter(datadate)
gen year = year(datadate)


tempfile frating
save "C:\Users\bhe2\Desktop\Yueran Ma\data\frating.dta", replace

*merge rating with original markit_size dataset 
use "C:\Users\bhe2\Dropbox (Chicago Booth)\FinancialFirms\data\cds\spread5y_size.dta" ,clear
merge m:m gvkey year qtr using "C:\Users\bhe2\Desktop\Yueran Ma\cds\data\frating.dta", keep(3) keepusing(frating)



gen fin = 1 /*general financial firms*/
gen sic2 = substr(sic,1,2) 
replace fin = 2 if sic =="6211" /*broker dealers*/
replace fin = 3 if sic2 == "60" /*commercial banks*/
replace fin = 4 if sic2 == "61" /*finance companies*/
replace fin = 5 if sic == "6282" /*asset managers*/


/*citibank exeption GVKEY 003243*/
replace fin = 3 if gvkey == "003243"
drop if sector != "Financials"


keep if fin == 2 |  fin == 3 |  fin == 4
keep if docclause =="MR"



gen log_ceqq = log(ceqq)
gen log_mkvaltq = log(mkvaltq)



cd "C:\Users\bhe2\Desktop\Yueran Ma\plot\spread5y_size\spread5y_bookassetvalue"

foreach i of num 2001/2014{
	foreach fin of num 2/4{
twoway (scatter spread5y log_ceqq, mlabel(ticker))(lfit spread5y log_ceqq ) if year== `i'  & fin == `fin' & spread5y <= 0.2, ///
ytitle(CDS spread) xtitle(Log Book Value of Assets) title(`i')
graph export spread5y_bav_`i'_fin`fin'.pdf,replace
	}
 }
 


cd "C:\Users\bhe2\Desktop\Yueran Ma\plot\spread5y_size\spread5y_marketvalue"
foreach i of num 2001/2014{
	foreach fin of num 2/4{
twoway (scatter spread5y log_mkvaltq, mlabel(ticker))(lfit spread5y log_mkvaltq ) if year== `i'  & fin == `fin' & spread5y <= 0.2, ///
ytitle(CDS spread) xtitle(Log Market Value) title(`i')

graph export spread5y_mv_`i'_fin`fin'.pdf,replace
	}
 }
save "C:\Users\bhe2\Desktop\Yueran Ma\cds\data\spread_size_rating.dta", replace 
 
 
 