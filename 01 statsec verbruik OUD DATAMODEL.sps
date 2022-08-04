* Encoding: windows-1252.
* waar staat de data.
DEFINE datamap () 'C:\github\fluvius-open-data\' !ENDDEFINE.
* waar staan de andere github projecten.
DEFINE github () 'C:\github\' !ENDDEFINE.
* jaartal waarvoor we werken.
DEFINE datajaar () '2020' !ENDDEFINE.

* opmerkingen:
verlichting: enkel in een paar gemeenten
enkele statsec in brussel, wallonië; niet-standaard gebieden onbekend
gecensureerd in gebied onbekend vs in een specifieke sector vs in sector REST > andere betekenis?


* bron: https://opendata.fluvius.be/explore/?q=verbruiksgegevens+per+statistische+sector&sort=modified

GET DATA
  /TYPE=XLSX
  /FILE=datamap + 'statistiche-sector-zonder-subsector-' + datajaar + '.xlsx'
  /SHEET=name 'ZS_RESULTAAT'
  /CELLRANGE=FULL
  /READNAMES=ON
  /DATATYPEMIN PERCENTAGE=95.0
  /HIDDEN IGNORE=YES.
EXECUTE.
DATASET NAME werkbestand WINDOW=FRONT.


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

DATASET ACTIVATE werkbestand.
DATASET DECLARE aggr.
AGGREGATE
  /OUTFILE='aggr'
  /BREAK=geoitem
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
sort cases geoitem (a).

MATCH FILES /FILE=*
  /FILE='uniekstatsec'
  /BY geoitem.
EXECUTE.
dataset close uniekstatsec.
string geolevel (a11).
compute geolevel="statsec2019".
compute period = number(datajaar,f4.0).

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
v2506_gas_nietres_n (missing=-99996).

EXECUTE.
delete variables gewest.

SAVE TRANSLATE OUTFILE='C:\github\fluvius\upload_elektriciteit_verbruik.xlsx'
  /TYPE=XLS
  /VERSION=12
  /MAP
  /FIELDNAMES VALUE=NAMES
  /CELLS=VALUES
/replace.

