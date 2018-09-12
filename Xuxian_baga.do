global wkdir = "XXXX"
cd wkdir

use "XXX.dta", replace

gen baga =1

replace baga = 0 if Bianca == 1;

merge m:m baga using "baga_list.dta"
keep if _merge == 3

preserve

use "YYY.dta", replace
reg Y X, r
estout ...

restore

sum baga
