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
Diese Daten kommen direkt aus dem ELK-Stack und werden summiert nach Jahr. 

#### e-codices Google Analytics Reports (jährlich) (noch nicht integriert/implementiert)
Diese Daten können direkt übder die Google Analytics Reporting v4 API geholt. Es liegt ein Report pro Seite & Jahr vor.
Die könnte einfach verfeinert werden (z.B. pro Monat), aber da andere Datenquellen ebenfalls nur pro Jahr vorhanden sind würde das aktuell nicht so viel Sinn machen. 

Diese Daten liegen seit Beginn von e-codices 2012 vor, je nachdem wie lange die Digitalisate dort waren.

Einige Digitalisate wurden von e-codices entfernt und auf e-manuscripta verschoben (Hochschulpolitischer Entscheid, hat aber zur Folge, dass bei ein paar Einträge die e-codices dois nicht mehr funkionieren und doppelt Einträge zu den Nutzerdaten vorhanden sind.)
#### e-rara & e-manuscripta Angelfish Reports (jährlich)
Werden direkt bereitgestellt nach Jahr & Pageviews. Müssen stark bereinigt und kombiniert werden, da die Seiten nur als VLID
verlinkung vorhanden sind. Es gibt ein mapping zwischen Systemnummer und VLID aber nur für die Titelseiten. Die restlichen wurden einfach in numerischer Reihenfolge angenommen (das heisst, eine VLID ohne mapping wurde der nächst kleineren VLID mit mapping angefügt, Stichprobenmässig scheint dies eine gute Annahme zu sein).

#### dsv01 Ausleihen & Reservationen (ARC-Statistik Tool)
Diese Daten werden direkt aus Aleph gelesen und liegen vn 2016 - 2018 pro Jahr vor. ARC-Reports könnte man automatisieren.

#### dsv05 Ausleihen (Webformular / E-mail) (noch nicht integriert/implementiert)
Es existieren E-Mails für die Ausleihen in dsv05 (2304 Stück, 2016 - 2018) welche einen Teil der Ausleihen in DSV05 abbilden.

## Datenmodel
Das verwendete Datenmodel ist sehr einfach gestrickt und resultiert aus den vorhandenen Daten und Anforderungen des Projektes.

Die Basisklasse des Models ist ein Titel. Jeder Titel hat einen Eintrag im swissbib Index. Hinter einem Titel kann sich alles
Mögliche verstecken:

- Ein einzelnes Foto
- ein paar lose Blätter
- ein gebundenes Buch
- eine ganze Zeitung

Worum es sich handelt kann i.d.R. am Format der Instanz definiert werden. Dies kann aber nicht garantiert werden. Dies ist 
wichtig um die Anzahl Seiten schätzen zu können falls diese nicht in den Daten vorhanden ist.
