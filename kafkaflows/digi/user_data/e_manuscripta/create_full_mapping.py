import json
import requests
import time


def create_full_mapping(path: str, collection: str, start: int): 
  with open(path, 'r') as fp:
    vlids_dict = dict()
    vlids = list()

    for line in fp:
      vlid, sys_number = line.strip().split(',')
      vlids_dict[vlid] = sys_number
      vlids.append(int(vlid))

    vlids.sort()

    with open("output-{}.json".format(collection), 'a') as fp:
      for x, value in enumerate(vlids[:-2]):
        current_vlid = int(value)
        if current_vlid > start:
          next_vlid = int(vlids[x + 1])
          temp = dict()
          temp[vlids_dict[str(value)]] = list()
          count_404 = 0
          for x in range(current_vlid, next_vlid):
            try:
              response = requests.get("https://www.e-manuscripta.ch/id/{}".format(x))
            except requests.exceptions.ConnectionError:
              time.sleep(10000)
              try:
                response = requests.get("https://www.e-manuscripta.ch/id/{}".format(x))
              except requests.exceptions.ConnectionError:
                continue
            if response.ok:
              temp[vlids_dict[str(value)]].append(x)
            else:
              print(x)
              count_404 += 1
              if count_404 == 2:
                break
          text = json.dumps(temp)
          fp.write(text + "\n")


create_full_mapping("mapping/emanus-bau-mapping.csv", 'bau', 0)
create_full_mapping("mapping/emanus-swa-mapping.csv", 'swa', 0)