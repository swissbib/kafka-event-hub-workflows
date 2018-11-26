from simple_elastic import ElasticIndex
import json

def import_data(plattform: str, year: str):

  index = ElasticIndex("emanus-{}-data-base-{}".format(plattform, year), "doc")
  
  with open("emanus-{}-{}.json".format(plattform, year), 'r') as fp:
    text = json.load(fp)
    metadata = dict()
    data = list()
    for key in text:
      if key == "data":
        for item in text[key]:
          result = item
          result["identifier"] = item["dimensions"]["pagestem"]
          data.append(result)
      else:
        metadata[key] = text[key]
    index.bulk(data, identifier_key="identifier")
    index.index_into(metadata, 0)


if __name__ == '__main__':
  import_data("bau", "2016")
  import_data("bau", "2017")
  import_data("bau", "2018")
  import_data("swa", "2016")
  import_data("swa", "2017")
  import_data("swa", "2018")