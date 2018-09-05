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



The Kafka consumer filters, transforms and enriches the data
collected and then indexes it into a elasticsearch cluster.

Elastic is then used to analyze and visualize the data.

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