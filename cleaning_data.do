
clear all
* for import .sav (spss files) into stata
net from "http://radyakin.org/transfer/usespss/beta" //command: usespss

* or ssc install importsav. We previously need to install R on computer.
ssc install importsav

/*

Los sectores con tipo M se refieren a las empresas debajo de las medianas.
Por ello no se tomaran en cuenta en la presente investigación.

Sector		Nombre				Tipo
01 		Agencias de viaje 		B1
02 		Agroindustria			F2 M
03 		Centros Educativos NE	U
04 		Comercio				F2 M
05 		Construcción			F2 M
07 		Establec. Hospedaje		A1
08 		Hidrocarburos			U
10 		Pesca					A (acuicultura) P (pesca)
11 		Manufactura				D2 M
12 		Servicios eléctricos	U
13 		Transportes&comunicac.	F2 M
14 		Universidades NE		U
15 		Restaurantes			R2
16 		Servicios				F2 M

*/

set more off

global main "D:\Research_Projects\Tesis\Estimation"
global data "D:\Research_Projects\Bases\eea\data"
global temp "$main\temp"
global output "$main\output"
global tables "$main\tables"
global figures "$main\figures"

foreach var in s01_fB1 s02_fF2 s03_fU s04_fF2 s05_fF2 s07_fA1 ///
				s08_fU s10_fA s10_fP s12_fU s13_fF2 s15_fR2 s16_fF2 {
forvalue i = 2012/2018 {

******************* Base muestral del año respectivo ***************************

import dbase using "$data\a`i'_CAP_01.dbf", clear

gen year = `i'
gen ubigeo = CCDD +  CCPP + CCDI
gen fundation = FECANIVERS
destring fundation, replace
gen locales = 1

preserve
collapse (sum) locales, by(IRUC year)
save "$output\temp.dta", replace
restore

merge m:1 IRUC year using "$output\temp.dta"

keep if _m==3
drop _m

duplicates drop IRUC year, force

keep IRUC CIIU year ubigeo fundation locales CODSECTOR CODFORMATO FACTOR

save "$output\temp.dta", replace

***************************** 02. Hoja de Balance *******************************

import dbase using "$data\a`i'_`var'_c02_1.dbf", clear

gen year = `i'

gen money_bank = P01 if CLAVE == "001"
gen assets_st = P01 if CLAVE == "011"
gen assets_lt = P01 if CLAVE == "029"
gen assets_tot = P01 if CLAVE == "030"
gen taxes_pensions = P01 if CLAVE == "033"
gen salaries = P01 if CLAVE == "034"
gen bills_pendings = P01 if CLAVE == "035"
gen liability_st = P01 if CLAVE == "041"
gen liability_lt = P01 if CLAVE == "050"
gen capital = P01 if CLAVE == "051"
gen results_cum = P01 if CLAVE == "057"
gen results_cum2 = P01 if CLAVE == "058"
gen results_current = P01 if CLAVE == "059"
gen patrimony = P01 if CLAVE == "060"
gen liability_patrim = P01 if CLAVE == "061"

collapse (firstnm) money_bank assets_* taxes_* salaries bills_pendings ///
					liability_* capital results_* patrimony, ///
					by(IRUC CODSECTOR CODFORMATO year)

merge 1:m IRUC year using "$output\temp.dta"

keep if _m ==3
drop _m

save "$output\temp.dta", replace

************************ 03. Estado de Resultados *******************************

import dbase using "$data\a`i'_`var'_c03_1.dbf", clear

gen year = `i'
* Estado de resultados por naturaleza (CAP 03)
gen sales = P01 if CLAVE == "006"
gen output_current = P01 if CLAVE == "011"
gen output_tot = P01 if CLAVE == "012"
gen consumption = P01 if CLAVE == "028"
gen VA = P01 if CLAVE == "029"
gen high_skills = P01 if CLAVE == "030"
gen net_tax = P01 if CLAVE == "031"
gen gross_oper_surplus = P01 if CLAVE == "034" //Excedente bruto de explotación
gen oper_income = P01 if CLAVE == "053"  //Ingreso Operativo 
gen EBT = P01 if CLAVE == "058"
gen corporate_tax = P01 if CLAVE == "060"
gen net_utility = P01 if CLAVE == "061"

collapse (firstnm) sales output_* consumption VA high_skills gross_oper_surplus ///
					oper_income EBT corporate_tax net_utility, ///
					by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

keep if _m == 3
drop _m

save "$output\temp.dta", replace

****************************** 07. Impuestos ************************************

import dbase using "$data\a`i'_`var'_c07_1.dbf", clear

drop if CLAVE == "002" | CLAVE == "004"

gen year = `i'

gen igv_sales = P01 if CLAVE == "001"
gen igv_buy = P01 if CLAVE == "003"
gen igv_net = P01 if CLAVE == "005"

gen isc_sales = P01 if CLAVE == "001"
gen isc_buy = P01 if CLAVE == "003"
gen isc_net = P01 if CLAVE == "005"

collapse (firstnm) igv_* isc_*,	by(IRUC CODSECTOR CODFORMATO year)

replace igv_net = igv_sales - igv_buy if (igv_net == . | igv_net == 0)
replace isc_net = isc_sales - isc_buy if (isc_net == . | isc_net == 0)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************ 08. Pagos de dividendos ********************************

import dbase using "$data\a`i'_`var'_c08_1.dbf", clear

keep if CLAVE == "001" | CLAVE == "002" | CLAVE == "003"

gen year = `i'

gen dividend_pending = P01 if CLAVE == "001"
gen dividend_paid = P01 if CLAVE == "002"
gen dividend_domestic = P01 if CLAVE == "003"
gen dividend_foreign = P01 if CLAVE == "004"
destring dividend_*, replace

collapse (firstnm) dividend_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************ 09. Capital *********************************************

import dbase using "$data\a`i'_`var'_c09_1.dbf", clear

keep if CLAVE == "001" | CLAVE == "002"

gen year = `i'

gen capital_declared = P01 if CLAVE == "001"
gen capital_paid = P01 if CLAVE == "002"

collapse (firstnm) capital_*, by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

******************* 10. Salarios y beneficios sociales ***************************

if "`var'" != "s03_fU" & `i' != 2015   import dbase using "$data\a`i'_`var'_c10_1.dbf", clear
if "`var'" != "s03_fU" & `i' == 2015   import dbase using "$data\a`i'_`var'_c10_1.dbf", clear
if "`var'" == "s03_fU" & `i' != 2015   import dbase using "$data\a`i'_`var'_c10_1.dbf", clear
if "`var'" == "s03_fU" & `i' == 2015 import dbase using "$data\a`i'_`var'_c10.dbf", clear

gen year = `i'
gen salary = P01 if CLAVE == "001"
gen wage = P01 if CLAVE == "002"
gen social = P01 if CLAVE == "008"
gen pension = P01 if CLAVE == "009"

collapse (firstnm) salary wage social pension,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************** 11. Mano de Obra usada ******************************


if "`var'" == "s03_fU" & `i' != 2015 {
	import dbase using "$data\a`i'_`var'_c11_1.dbf", clear
	}
else if "`var'" == "s03_fU" & `i' == 2015 {
import dbase using "$data\a`i'_`var'_c11.dbf", clear
}
else if "`var'" == "s02_fF2" {
import dbase using "$data\a`i'_s02_fF2_c08BE_2.dbf", clear
}
else {
import dbase using "$data\a`i'_`var'_c11_1.dbf", clear
}

gen year = `i'

gen labor_skilled = P01 if CLAVE == "001"
gen labor_permanent = P01 if CLAVE == "002"
gen labor_tot = P01 if CLAVE == "006"

gen labor_skilled_m = P02 if CLAVE == "001"
gen labor_permanent_m = P02 if CLAVE == "002"
gen labor_tot_m = P02 if CLAVE == "006"

gen labor_skilled_f = P01 if CLAVE == "001"
gen labor_permanent_f = P01 if CLAVE == "002"
gen labor_tot_f = P01 if CLAVE == "006"

collapse (firstnm) labor_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

****************************** 13. Ventas ***************************************
/*
import dbase using "$data\a`i'_s01_fB1_c13_1.dbf", clear

gen year = `i'
gen sales_v2 = P02 if CLAVE == "011"
gen sales_core = P02 if CLAVE == "001"

collapse (firstnm) sales_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m
*/

if "`var'" == "s01_fB1" & `i' == 2012 save "$output\agencias.dta", replace
if "`var'" == "s01_fB1" & `i' != 2012 append using "$output\agencias.dta"
if "`var'" == "s01_fB1" & `i' != 2012 save "$output\agencias.dta", replace

if "`var'" == "s02_fF2" & `i' == 2012 save "$output\agroindustria.dta", replace
if "`var'" == "s02_fF2" & `i' != 2012 append using "$output\agroindustria.dta"
if "`var'" == "s02_fF2" & `i' != 2012 save "$output\agroindustria.dta", replace

if "`var'" == "s03_fU" & `i' == 2012 save "$output\escuelas.dta", replace
if "`var'" == "s03_fU" & `i' != 2012 append using "$output\escuelas.dta"
if "`var'" == "s03_fU" & `i' != 2012 save "$output\escuelas.dta", replace

if "`var'" == "s04_fF2" & `i' == 2012 save "$output\comercio.dta", replace
if "`var'" == "s04_fF2" & `i' != 2012 append using "$output\comercio.dta"
if "`var'" == "s04_fF2" & `i' != 2012 save "$output\comercio.dta", replace

if "`var'" == "s05_fF2" & `i' == 2012 save "$output\construcción.dta", replace
if "`var'" == "s05_fF2" & `i' != 2012 append using "$output\construcción.dta"
if "`var'" == "s05_fF2" & `i' != 2012 save "$output\construcción.dta", replace

if "`var'" == "s07_fA1" & `i' == 2012 save "$output\hospedaje.dta", replace
if "`var'" == "s07_fA1" & `i' != 2012 append using "$output\hospedaje.dta"
if "`var'" == "s07_fA1" & `i' != 2012 save "$output\hospedaje.dta", replace

if "`var'" == "s08_fU" & `i' == 2012 save "$output\hidrocarburos.dta", replace
if "`var'" == "s08_fU" & `i' != 2012 append using "$output\hidrocarburos.dta"
if "`var'" == "s08_fU" & `i' != 2012 save "$output\hidrocarburos.dta", replace

if "`var'" == "s10_fA" & `i' == 2012 save "$output\acuicultura.dta", replace
if "`var'" == "s10_fA" & `i' != 2012 append using "$output\acuicultura.dta"
if "`var'" == "s10_fA" & `i' != 2012 save "$output\acuicultura.dta", replace

if "`var'" == "s10_fP" & `i' == 2012 save "$output\pesca.dta", replace
if "`var'" == "s10_fP" & `i' != 2012 append using "$output\pesca.dta"
if "`var'" == "s10_fP" & `i' != 2012 save "$output\pesca.dta", replace

if "`var'" == "s12_fU" & `i' == 2012 save "$output\electricidad.dta", replace
if "`var'" == "s12_fU" & `i' != 2012 append using "$output\electricidad.dta"
if "`var'" == "s12_fU" & `i' != 2012 save "$output\electricidad.dta", replace

if "`var'" == "s13_fF2" & `i' == 2012 save "$output\transporte.dta", replace
if "`var'" == "s13_fF2" & `i' != 2012 append using "$output\transporte.dta"
if "`var'" == "s13_fF2" & `i' != 2012 save "$output\transporte.dta", replace

if "`var'" == "s15_fR2" & `i' == 2012 save "$output\restaurantes.dta", replace
if "`var'" == "s15_fR2" & `i' != 2012 append using "$output\restaurantes.dta"
if "`var'" == "s15_fR2" & `i' != 2012 save "$output\restaurantes.dta", replace

if "`var'" == "s16_fF2" & `i' == 2012 save "$output\servicios.dta", replace
if "`var'" == "s16_fF2" & `i' != 2012 append using "$output\servicios.dta"
if "`var'" == "s16_fF2" & `i' != 2012 save "$output\servicios.dta", replace

}
}
*

******************************* Universidades No Estatales *********************

forvalue i = 2012/2018 {

******************* Base muestral del año respectivo ***************************

import dbase using "$data\a`i'_CAP_01.dbf", clear

gen year = `i'
gen ubigeo = CCDD +  CCPP + CCDI
gen fundation = FECANIVERS
destring fundation, replace
gen locales = 1

preserve
collapse (sum) locales, by(IRUC year)
save "$output\temp.dta", replace
restore

merge m:1 IRUC year using "$output\temp.dta"

keep if _m==3
drop _m

duplicates drop IRUC year, force

keep IRUC CIIU year ubigeo fundation locales CODSECTOR CODFORMATO FACTOR

save "$output\temp.dta", replace

***************************** 02. Hoja de Balance *******************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c02_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c02_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

gen money_bank = P01 if CLAVE == "001"
gen assets_st = P01 if CLAVE == "011"
gen assets_lt = P01 if CLAVE == "029"
gen assets_tot = P01 if CLAVE == "030"
gen taxes_pensions = P01 if CLAVE == "033"
gen salaries = P01 if CLAVE == "034"
gen bills_pendings = P01 if CLAVE == "035"
gen liability_st = P01 if CLAVE == "041"
gen liability_lt = P01 if CLAVE == "050"
gen capital = P01 if CLAVE == "051"
gen results_cum = P01 if CLAVE == "057"
gen results_cum2 = P01 if CLAVE == "058"
gen results_current = P01 if CLAVE == "059"
gen patrimony = P01 if CLAVE == "060"
gen liability_patrim = P01 if CLAVE == "061"

collapse (firstnm) money_bank assets_* taxes_* salaries bills_pendings ///
					liability_* capital results_* patrimony, ///
					by(IRUC CODSECTOR CODFORMATO year)

merge 1:m IRUC year using "$output\temp.dta"

keep if _m ==3
drop _m

save "$output\temp.dta", replace

************************ 03. Estado de Resultados *******************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c03_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c03_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

* Estado de resultados por naturaleza (CAP 03)
gen sales = P01 if CLAVE == "006"
gen output_current = P01 if CLAVE == "011"
gen output_tot = P01 if CLAVE == "012"
gen consumption = P01 if CLAVE == "028"
gen VA = P01 if CLAVE == "029"
gen high_skills = P01 if CLAVE == "030"
gen net_tax = P01 if CLAVE == "031"
gen gross_oper_surplus = P01 if CLAVE == "034" //Excedente bruto de explotación
gen oper_income = P01 if CLAVE == "053"  //Ingreso Operativo 
gen EBT = P01 if CLAVE == "058"
gen corporate_tax = P01 if CLAVE == "060"
gen net_utility = P01 if CLAVE == "061"

collapse (firstnm) sales output_* consumption VA high_skills gross_oper_surplus ///
					oper_income EBT corporate_tax net_utility, ///
					by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

keep if _m == 3
drop _m

save "$output\temp.dta", replace

****************************** 07. Impuestos ************************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c07_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c07_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO
drop if CLAVE == "002" | CLAVE == "004"

gen igv_sales = P01 if CLAVE == "001"
gen igv_buy = P01 if CLAVE == "003"
gen igv_net = P01 if CLAVE == "005"

gen isc_sales = P01 if CLAVE == "001"
gen isc_buy = P01 if CLAVE == "003"
gen isc_net = P01 if CLAVE == "005"

collapse (firstnm) igv_* isc_*,	by(IRUC CODSECTOR CODFORMATO year)

replace igv_net = igv_sales - igv_buy if (igv_net == . | igv_net == 0)
replace isc_net = isc_sales - isc_buy if (isc_net == . | isc_net == 0)


merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************ 08. Pagos de dividendos ********************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c08_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c08_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

keep if CLAVE == "001" | CLAVE == "002" | CLAVE == "003"

gen dividend_pending = P01 if CLAVE == "001"
gen dividend_paid = P01 if CLAVE == "002"
gen dividend_domestic = P01 if CLAVE == "003"
gen dividend_foreign = P01 if CLAVE == "004"
destring dividend_*, replace

collapse (firstnm) dividend_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************ 09. Capital *********************************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c09_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c09_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

keep if CLAVE == "001" | CLAVE == "002"

gen capital_declared = P01 if CLAVE == "001"
gen capital_paid = P01 if CLAVE == "002"

collapse (firstnm) capital_*, by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

******************* 10. Salarios y beneficios sociales ***************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c10_1.dbf", clear
if `i' == 2016 importsav  "$data\a`i'_s14_fU_c10_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

gen salary = P01 if CLAVE == "001"
gen wage = P01 if CLAVE == "002"
gen social = P01 if CLAVE == "008"
gen pension = P01 if CLAVE == "009"

collapse (firstnm) salary wage social pension,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

************************** 11. Mano de Obra usada ******************************

if `i' != 2016 import dbase using "$data\a`i'_s14_fU_c11_1.dbf", clear
if `i' == 2016 importsav "$data\a`i'_s14_fU_c11_1.sav"

gen year = `i'
if year == 2016 rename Clave CLAVE
if year == 2016 rename CodSector CODSECTOR
if year == 2016 rename CodFormato CODFORMATO

gen labor_skilled = P01 if CLAVE == "001"
gen labor_permanent = P01 if CLAVE == "002"
gen labor_tot = P01 if CLAVE == "006"

gen labor_skilled_m = P02 if CLAVE == "001"
gen labor_permanent_m = P02 if CLAVE == "002"
gen labor_tot_m = P02 if CLAVE == "006"

gen labor_skilled_f = P01 if CLAVE == "001"
gen labor_permanent_f = P01 if CLAVE == "002"
gen labor_tot_f = P01 if CLAVE == "006"

collapse (firstnm) labor_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m

save "$output\temp.dta", replace

****************************** 13. Ventas ***************************************
/*
import dbase using "$data\a`i'_s01_fB1_c13_1.dbf", clear

gen year = `i'
gen sales_v2 = P02 if CLAVE == "011"
gen sales_core = P02 if CLAVE == "001"

collapse (firstnm) sales_*,	by(IRUC CODSECTOR CODFORMATO year)

merge 1:1 IRUC year using "$output\temp.dta"

drop if _m == 1
drop _m
*/

if `i' == 2012 save "$output\universidades.dta", replace
if `i' != 2012 append using "$output\universidades.dta"
if `i' != 2012 save "$output\universidades.dta", replace
}
*


local sectores agencias agroindustria escuelas comercio construcción hospedaje ///
				hidrocarburos acuicultura pesca electricidad transporte ///
				restaurantes servicios universidades


use "$output\agencias.dta", clear

foreach var of local sectores {
append using "$output\\`var'.dta"
}
*
drop FACTOR
save "$output\EEA-2012-2018.dta", replace

