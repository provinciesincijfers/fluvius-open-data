* Encoding: windows-1252.
* waar staat de data.
DEFINE datamap () 'C:\github\fluvius\' !ENDDEFINE.
* waar staan de andere github projecten.
DEFINE github () 'C:\github\' !ENDDEFINE.

* opmerkingen:
verlichting: enkel in een paar gemeenten
enkele statsec in brussel, wallonië; niet-standaard gebieden onbekend
gecensureerd in gebied onbekend vs in een specifieke sector vs in sector REST > andere betekenis?


* bron: https://opendata.fluvius.be/explore/?q=verbruiksgegevens+per+statistische+sector&sort=modified



PRESERVE.
SET DECIMAL DOT.

GET DATA  /TYPE=TXT
  /FILE=datamap + "1_06b-verbruiksgegevens-statistische-sector-met-nace-sector.csv"
  /DELIMITERS=";"
  /ARRANGEMENT=DELIMITED
  /FIRSTCASE=2
  /DATATYPEMIN PERCENTAGE=95.0
  /VARIABLES=
  Verbruiksjaar f4.0
  Markt a13
  Richting a8
  StatistischeSector a9
  Provincie a30
  Gemeente a30
  Sector a30
  RolContact a10
  Aantal a15
  BenaderendVerbruikkWh a15
  /MAP.
RESTORE.
CACHE.
EXECUTE.
DATASET NAME werkbestand WINDOW=FRONT.

* slechte getalnotatie afhandelen (. zowel voor duizendtallen als voor nutteloze 0 na de komma).
compute aantal=rtrim(rtrim(rtrim(aantal),"0"),".").
compute aantal=replace(aantal,".","").
* opkuisen omdat spss valt over . als kommagetal.
compute BenaderendVerbruikkWh=REPLACE(BenaderendVerbruikkWh,".",",").
alter type aantal (f8.0).
alter type BenaderendVerbruikkWh (f10.2).

* inconsistenties over de jaren fixen.
compute sector=upcase(sector).
recode Markt ("Aardgas"="Gas").

* opkuisen statsec.
compute StatistischeSector=upcase(StatistischeSector).
* REST en ZZZZ lijken beide gewoon "gebied onbekend".
if char.index(StatistischeSector,"REST")>0 StatistischeSector=concat(char.substr(StatistischeSector,1,5),"ZZZZ").
rename variables StatistischeSector=geoitem.
sort cases geoitem (a).

* er zitten enkele sectoren buiten Vlaanderen en Brussel in.
GET
  FILE=github + 'gebiedsniveaus\verzamelbestanden\verwerkt_alle_gebiedsniveaus.sav'.
DATASET NAME allegebieden WINDOW=FRONT.

DATASET ACTIVATE allegebieden.
DATASET DECLARE uniekstatsec.
AGGREGATE
  /OUTFILE='uniekstatsec'
  /BREAK=statsec2019 gewest
  /N_BREAK=N.
dataset activate uniekstatsec.
dataset close allegebieden.
delete variables N_BREAK.
rename variables statsec2019=geoitem.

DATASET ACTIVATE uniekstatsec.
FILTER OFF.
USE ALL.
SELECT IF (geoitem ~= "" & geoitem~="99999ZZZZ" & geoitem~="99991ZZZZ" & geoitem~="99992ZZZZ").
EXECUTE.

DATASET ACTIVATE werkbestand.
MATCH FILES /FILE=*
  /TABLE='uniekstatsec'
  /BY geoitem.
EXECUTE.
* uniekstatsec nog niet sluiten, gebruiken we straks opnieuw.

* er vanuitgaande dat dit fouten in de verwerking zijn en in feite werkelijk Vlaams verbruik is, voegen we deze allemaal toe aan "Vlaanderen gebied onbekend".
if missing(gewest) | gewest=4000 geoitem="99991ZZZZ".


if Richting="Afname" & Markt="Elektriciteit" & sector~='HUISHOUDENS' v2506_elek_nietres_cons=BenaderendVerbruikkWh.
if Richting="Afname" & Markt="Elektriciteit" & sector='HUISHOUDENS' v2506_elek_res_cons=BenaderendVerbruikkWh.
if Richting="Afname" & Markt="Elektriciteit" & sector~='HUISHOUDENS' v2506_elek_nietres_n=Aantal.
if Richting="Afname" & Markt="Elektriciteit" & sector='HUISHOUDENS' v2506_elek_res_n=Aantal.
if Richting="Injectie" & Markt="Elektriciteit" v2506_elek_res_injectie=BenaderendVerbruikkWh.

if Markt="Gas" & sector='HUISHOUDENS' v2506_gas_res_cons=BenaderendVerbruikkWh.
if Markt="Gas" & sector~='HUISHOUDENS' v2506_gas_nietres_cons=BenaderendVerbruikkWh.
if Markt="Gas" & sector='HUISHOUDENS' v2506_gas_res_n=Aantal.
if Markt="Gas" & sector~='HUISHOUDENS' v2506_gas_nietres_n=Aantal.

rename variables Verbruiksjaar = period.

DATASET ACTIVATE werkbestand.
DATASET DECLARE aggr.
AGGREGATE
  /OUTFILE='aggr'
  /BREAK=geoitem period
/v2506_elek_nietres_cons=sum(v2506_elek_nietres_cons)
/v2506_elek_res_cons=sum(v2506_elek_res_cons)
/v2506_elek_nietres_n=sum(v2506_elek_nietres_n)
/v2506_elek_res_n=sum(v2506_elek_res_n)
/v2506_elek_res_injectie=sum(v2506_elek_res_injectie)
/v2506_gas_res_cons=sum(v2506_gas_res_cons)
/v2506_gas_nietres_cons=sum(v2506_gas_nietres_cons)
/v2506_gas_res_n=sum(v2506_gas_res_n)
/v2506_gas_nietres_n=sum(v2506_gas_nietres_n).
dataset activate aggr.
sort cases geoitem (a) period (a).

dataset activate uniekstatsec.
compute period=2018.
dataset copy copy.
dataset activate copy.
compute period=2019.
DATASET ACTIVATE uniekstatsec.
ADD FILES /FILE=*
  /FILE='copy'.
EXECUTE.
dataset activate copy.
compute period=2020.
DATASET ACTIVATE uniekstatsec.
ADD FILES /FILE=*
  /FILE='copy'.
EXECUTE.
dataset activate copy.
compute period=2021.
DATASET ACTIVATE uniekstatsec.
ADD FILES /FILE=*
  /FILE='copy'.
EXECUTE.
dataset close copy.
sort cases geoitem (a) period (a).

MATCH FILES /FILE=*
  /FILE='aggr'
  /BY geoitem period.
EXECUTE.

string geolevel (a11).
compute geolevel="statsec2019".


do if gewest=4000.
recode 
v2506_elek_nietres_cons
v2506_elek_res_cons
v2506_elek_nietres_n
v2506_elek_res_n
v2506_elek_res_injectie
v2506_gas_res_cons
v2506_gas_nietres_cons
v2506_gas_res_n
v2506_gas_nietres_n (else=-99999).
end if.

recode v2506_elek_nietres_cons
v2506_elek_res_cons
v2506_elek_nietres_n
v2506_elek_res_n
v2506_elek_res_injectie
v2506_gas_res_cons
v2506_gas_nietres_cons
v2506_gas_res_n
v2506_gas_nietres_n (missing=0).

EXECUTE.
delete variables gewest.

sort cases period (a) geoitem (a).

SAVE TRANSLATE OUTFILE='C:\github\fluvius\upload_elektriciteit_verbruik.xlsx'
  /TYPE=XLS
  /VERSION=12
  /MAP
  /FIELDNAMES VALUE=NAMES
  /CELLS=VALUES
/replace.

* test op gekke evoluties.

sort cases geoitem (a) period (a).
if geoitem=lag(geoitem) & (v2506_elek_res_n>100 | lag(v2506_elek_res_n)>100) groei_v2506_elek_res_n=v2506_elek_res_n/lag(v2506_elek_res_n)*100.
if geoitem=lag(geoitem) & (v2506_elek_nietres_n>100 | lag(v2506_elek_nietres_n)>100)  groei_v2506_elek_nietres_n=v2506_elek_nietres_n/lag(v2506_elek_nietres_n)*100.
if geoitem=lag(geoitem) & (v2506_gas_res_n>100 | lag(v2506_gas_res_n)>100)  groei_v2506_gas_res_n=v2506_gas_res_n/lag(v2506_gas_res_n)*100.
if geoitem=lag(geoitem)  & (v2506_gas_nietres_n>100 | lag(v2506_gas_nietres_n)>100) groei_v2506_gas_nietres_n=v2506_gas_nietres_n/lag(v2506_gas_nietres_n)*100.



recode groei_v2506_elek_res_n
groei_v2506_elek_nietres_n
groei_v2506_gas_res_n
groei_v2506_gas_nietres_n
(lowest through 50=-1) (200 through highest=1)
into groei_v2506_elek_res_n_check
groei_v2506_elek_nietres_n_check
groei_v2506_gas_res_n_check
groei_v2506_gas_nietres_n_check.
EXECUTE.

