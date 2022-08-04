* Encoding: windows-1252.
DEFINE datamap () 'C:\github\fluvius-open-data\' !ENDDEFINE.

https://www.fluvius.be/nl/thema/open-data/verbruiksgegevens-per-statistische-sector

GET DATA
  /TYPE=XLSX
  /FILE=datamap + 'statistiche-sector-zonder-subsector-2020.xlsx'
  /SHEET=name 'ZS_RESULTAAT'
  /CELLRANGE=FULL
  /READNAMES=ON
  /DATATYPEMIN PERCENTAGE=95.0
  /HIDDEN IGNORE=YES.
EXECUTE.
DATASET NAME werkbestand WINDOW=FRONT.

do if Richting="Afname".
do if Markt="Elektriciteit".
if sector='Empty/Onbekend' v2506_onbekend_elek_vb=BenaderendVerbruikkWh.
if sector='ENERGIESECTOR' v2506_energie_elek_vb=BenaderendVerbruikkWh.
if sector='HUISHOUDENS' v2506_huishoudens_elek_vb=BenaderendVerbruikkWh.
if sector='INDUSTRIE' v2506_industrie_elek_vb=BenaderendVerbruikkWh.
if sector='LANDBOUW, BOSBOUW EN VISSERIJ' v2506_landbouw_elek_vb=BenaderendVerbruikkWh.
if sector='Openbare verlichting' v2506_verlichting_elek_vb=BenaderendVerbruikkWh.
if sector='REST' v2506_gecensureerd_elek_vb=BenaderendVerbruikkWh.
if sector='TERTIAIRE SECTOR' v2506_tertiair_elek_vb=BenaderendVerbruikkWh.
if sector='TRANSPORT' v2506_transport_elek_vb=BenaderendVerbruikkWh.
if sector='Empty/Onbekend' v2506_onbekend_elek_n=Aantal.
if sector='ENERGIESECTOR' v2506_energie_elek_n=Aantal.
if sector='HUISHOUDENS' v2506_huishoudens_elek_n=Aantal.
if sector='INDUSTRIE' v2506_industrie_elek_n=Aantal.
if sector='LANDBOUW, BOSBOUW EN VISSERIJ' v2506_landbouw_elek_n=Aantal.
if sector='Openbare verlichting' v2506_verlichting_elek_n=Aantal.
if sector='REST' v2506_gecensureerd_elek_n=Aantal.
if sector='TERTIAIRE SECTOR' v2506_tertiair_elek_n=Aantal.
if sector='TRANSPORT' v2506_transport_elek_n=Aantal.
end if.
end if.
DATASET ACTIVATE werkbestand.
DATASET DECLARE aggr.
AGGREGATE
  /OUTFILE='aggr'
  /BREAK=StatistischeSector
  /v2506_onbekend_elek_vb=SUM(v2506_onbekend_elek_vb) 
  /v2506_energie_elek_vb=SUM(v2506_energie_elek_vb) 
  /v2506_huishoudens_elek_vb=SUM(v2506_huishoudens_elek_vb) 
  /v2506_industrie_elek_vb=SUM(v2506_industrie_elek_vb) 
  /v2506_landbouw_elek_vb=SUM(v2506_landbouw_elek_vb) 
  /v2506_verlichting_elek_vb=SUM(v2506_verlichting_elek_vb) 
  /v2506_gecensureerd_elek_vb=SUM(v2506_gecensureerd_elek_vb) 
  /v2506_tertiair_elek_vb=SUM(v2506_tertiair_elek_vb) 
  /v2506_transport_elek_vb=SUM(v2506_transport_elek_vb) 
  /v2506_onbekend_elek_n=SUM(v2506_onbekend_elek_n) 
  /v2506_energie_elek_n=SUM(v2506_energie_elek_n) 
  /v2506_huishoudens_elek_n=SUM(v2506_huishoudens_elek_n) 
  /v2506_industrie_elek_n=SUM(v2506_industrie_elek_n) 
  /v2506_landbouw_elek_n=SUM(v2506_landbouw_elek_n) 
  /v2506_verlichting_elek_n=SUM(v2506_verlichting_elek_n) 
  /v2506_gecensureerd_elek_n=SUM(v2506_gecensureerd_elek_n) 
  /v2506_tertiair_elek_n=SUM(v2506_tertiair_elek_n) 
  /v2506_transport_elek_n=SUM(v2506_transport_elek_n).
dataset activate aggr.

rename variables StatistischeSector=geoitem.
string geolevel (a9).
compute geolevel="statsec".
compute period = 2020.
EXECUTE.
recode v2506_onbekend_elek_vb
v2506_energie_elek_vb
v2506_huishoudens_elek_vb
v2506_industrie_elek_vb
v2506_landbouw_elek_vb
v2506_verlichting_elek_vb
v2506_gecensureerd_elek_vb
v2506_tertiair_elek_vb
v2506_transport_elek_vb
v2506_onbekend_elek_n
v2506_energie_elek_n
v2506_huishoudens_elek_n
v2506_industrie_elek_n
v2506_landbouw_elek_n
v2506_verlichting_elek_n
v2506_gecensureerd_elek_n
v2506_tertiair_elek_n
v2506_transport_elek_n (missing=-99996).


SAVE TRANSLATE OUTFILE='C:\github\fluvius\upload_elektriciteit_verbruik.xlsx'
  /TYPE=XLS
  /VERSION=12
  /MAP
  /FIELDNAMES VALUE=NAMES
  /CELLS=VALUES
/replace.

verlichting: enkel in een paar gemeenten
enkele statsec in brussel: 21017A09- en 21002A190
gecensureerd in gebied onbekend vs in een specifieke sector > andere betekenis?

aanpassen: enkel opdelen huishoudens/niet huishoudens

