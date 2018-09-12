use "C:\Users\bhe2\Desktop\wrds\WRDS Data\ccm_200101_201712.dta " , clear /*CCM is the CRSP-Compustat link. The dataset is on WRDS*/
 
gen order = 1 if linktype == "LC"
replace order = 2 if linktype == "LU"
replace order = 3 if linktype == "LN"
replace order = 4 if order ==.
 
bysort gvkey datadate: egen order_min = min(order)
keep if order == order_min
duplicates drop gvkey datadate, force
 
tempfile ccm
save "`ccm'"
 
use "C:\Users\bhe2\Desktop\wrds\WRDS Data\compustat.dta", clear /*place Compustat file here*/
 
merge m:1 gvkey datadate using "`ccm'", keep (1 3) keepusing(linktype lpermno lpermco)
drop _merge
ren lpermco permco

cd "C:\Users\bhe2\Desktop\wrds\WRDS Data"
outsheet using ccm_compustat.csv , comma

type ccm_compustat.csv
