## Project Usage of Digitisation's
 
This project has the following goals:

- Visualize the current inventory and archives of items 
published before 1920.
- Visualize the current status of what is digitized of 
this inventory.
- Analyse the usage of these digitized items on the 
publishing plattforms.
- Analyse what items of the inventory are searched, 
consulted or loaned.
  - To this end, the inventory should be categorized 
  by format and century. 

The first step is to collect, normalize and aggregate the 
bibliographic data of the inventory. The collection of 
bibliographic happens from sru.swissbib.ch, which offers a 
public SRU API. The API returns MARC JSON records. These 
records are then stored inside a Kafka topic as is.

Since the inventory of the University Library Basel is 
separated into two databases: dsv01 and dsv05.
The first is the main database with the majority of bibliographic records
and associated items. The second is primarily used for archival
items which cannot be taken home. There are some differences
on how bibliographic records are catalogued and how they are 
managed.

This leads to the separation of the work-flows to collect
the data on each of these databases. 

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