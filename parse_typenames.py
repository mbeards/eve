#!/usr/bin/env python


def parse_typenames(filepath):
  '''
  Read type information from types.txt, returning a dictionary containing
  typeid: (typename, typeclass, size, published, marketgroup, groupid, raceid)
  '''

  out = {}

  with open(filepath, 'r') as infile:

    for line in infile:
      l = line.split('|')
      if len(l) != 8:
        continue
      elif 'typeid' in l[0]:
        #Got header.
        continue
      else:
        #Got a line of data
        type_id = int(l[0].strip())
        type_name = l[1].strip()
        type_class = l[2].strip() #may be null
        if l[3].strip() != '':
          size = float(l[3].strip())
        else:
          size = 0
        published = True if l[4].strip() == '1' else False
        market_group = int(l[5].strip()) if l[5].strip() != '' else 0
        group_id = int(l[5].strip()) if l[6].strip() != '' else 0
        race_id = int(l[5].strip()) if l[7].strip() != '' else 0
        
        out[type_id] = {
            'typename': type_name, 
            'typeclass': type_class, 
            'size': size,
            'published': published,
            'market_group': market_group,
            'group_id': group_id,
            'race_id': race_id
        }

  return out


if __name__ == '__main__':

  print parse_typenames('data/meta/types.txt')
