## Projekt: Datensammlung zum Bestand & Nutzung von Titeln älter als 1920

Das Projekt hat die folgenden Ziele:

 - Sammlung & Visualiserung des aktuellen Bestandes
 - Visualisierung des Standes der Digitalisierung im Bestand.
 - Analyse der Nutzung der Digitaliste. . 
 - Anaylse der Suche und Ausleihe in den Katalogen.
 
Der erste Schritt dazu war das sammeln, normalisieren und aggregieren der
bibliografischen Metadaten. Diese Metadaten werden zentral von der 
[SRU Schnittstelle](https://sru.swissbib.ch) geholt und zuerst einmal 
in Kafka Event Hub zwischengelagert.

Das Inventar der Universitätsbibliothek ist primär in zwei verschiedenen Datenbanken 
katalogisiert (dsv01 & dsv05). Zwischen den Datenbanken gibt es unterschiede wie 
die Katalogisate erstellt werden (RDA vs. Eigenes). Deshalb gibt es einen eigenen Workflow für 
beide Datenbanken.

Die Kafka Consumers nehmen dann die Daten aus Kafka und Filtern & Transformieren sie in etwas neues. 
Ein paar Felder werden auch noch von anderen Datenquellen angereichert. So werden sie 
anschliessend in einen elasticsearch Index geschrieben um dort die Analysen und Visualisierungen mit
Kibana zu machen.

## Datenquellen
Die Daten werden aus den verschiedensten Quellen bezogen und sind sehr heterogen.

### Bibliografische Metadaten
Die Metadaten kommen aktuell fast komplett aus dem swissbib Index über die SRU Schnittstelle. Sie werden angereichert wo möglich
mit Daten aus dem digidata Index. Der digidata Index ist ein UB-interner Index welcher Daten aus dem 
Digitalisierungs-Workflow vereint.

### Nutzerdaten
Hier gibt es verschiedene Quellen mit verschiedenen Bedeutungen:

#### OPAC Access Logs (der alte Katalg der UB welcher immernoch läuft)
Die OPAC Access Logs wurden einmalig ausgewertet und liegen als Textdatei vor. Aktuell sind hier die Daten von Januar 2016 - August 2018 vorhanden.

Es werden nicht komplett alle Log-Einträge ausgewertet sondern nur ein bestimmter Eintrag, welcher pro Klick generiert wird
und die Systemnummer der Aufgerufenen Seite enthält. Jeder dieser Einträge enthält den Ausdruck `func=load-sfx-data` und `doc_number=`. Nach doc_number kommt dann die Systemnummer.

Das Problem an dieser Systemnummer ist, dass diese sowohl von DSV01 als auch DSV05 stammen könnte. Dies wird aktuell ignoriert und der Wert wird beiden Systemnummern zugewiesen sofern es aus beiden kommt.

#### Swissbib Access Logs (ELK-Stack)
#### e-codices Google Analytics Reports (jährlich)
#### e-rara & e-manuscripta Angelfish Reports (jährlich)
#### dsv05 Ausleihen & Reservationen (ARC-Statistik Tool)


## Datenmodel


Information on loans and reservations come from the ALEPH 
statistics tool ARC. For now just a one time dump, but this 
may be automated in the future (might not be worth it, until
the migration to ALMA has been completed)

The opac sub-module deals with the aggregation of Apache access
logs of the library's old web-catalogue, which is still in use.
Each click on a records title and other pages, generates a single
request where the system number is visible. These are 
collected and indexed in elasticsearch as well.


Nutzungsanalyse:

Was ist da:
OPAC Access Hits 2016 - 08.2018
Swissbib (green, orange, jus & sru) 12.2017 - present
Ausleihen + Reservationen im dsv01


Was fehlt:
Kopieraufträge
Ausleihen dsv05
Fernleihen
