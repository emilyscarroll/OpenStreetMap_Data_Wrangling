import xml.etree.cElementTree as ET
from collections import defaultdict
import pprint
import re
import csv
import codecs
import cerberus
import schema


OSMFILE = "sample0511.osm"


#regular expression
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Lane", "Street", "Pike", "Avenue", "Circle", "Pass", "Boulevard", "Highway",
            "Freeway", "Drive", "Court", "Place", "Way", "Road"]
            
mapping = {"St": "Street", "Hwy": "Highway", "lane": "Lane", "Rd": "Road"}


#begin auditing street types and names
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit(OSMFILE):
    osm_file = open(OSMFILE, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(OSMFILE, events=("start",)):

        if elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
        osm_file.close()
        return street_types
 
#cleaning data by updating street names
def update_name(name, mapping):

    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            name = re.sub(street_type_re, mapping[street_type], name)

    return name
    
#setup for shaping
sample = "sample0511.osm"
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


#shaping node and way elements into python dicts
def shape_element(element, node_attr_fields, way_attr_fields,
                  problem_chars, default_tag_type, OSMFILE):
            
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    if element.tag == 'node':
        for item in NODE_FIELDS:
            node_attribs[item] = element.attrib[item]
            
        for child in element.iter('tag'):
            if PROBLEMCHARS.match(child.attrib['k']):
                break

            else:
                
                st_types = audit(OSMFILE)
                for st_type, nodes in st_types.items():
                    for name in nodes:
                        update_name(name, mapping)
                        
                tag_dict = {}
                tag_dict['id'] = element.attrib['id']
                tag_dict['value'] = child.attrib['v']
                if ':' in child.attrib['k']:
                    k_value = child.attrib['k'].split(':',1)
                    tag_dict['type'] = k_value[0]
                    tag_dict['key'] = k_value[1]
                else:
                    tag_dict['key'] = child.attrib['k']
                    tag_dict['type'] = default_tag_type
                tags.append(tag_dict)
                
        return {'node': node_attribs, 'node_tags': tags}
    
    elif element.tag == 'way':
        
        for item in WAY_FIELDS:
            way_attribs[item] = element.attrib[item]

        for child in element.iter('tag'):
            if PROBLEMCHARS.match(child.attrib['k']):
                break
            else:
                
                st_types = audit(OSMFILE)
                for st_type, ways in st_types.items():
                    for name in ways:
                        update_name(name, mapping)
                        
                nd_dict = {}
                nd_dict['id'] = element.attrib['id']
                nd_dict['value'] = child.attrib['v']
                if ':' in child.attrib['k']:
                    k_value = child.attrib['k'].split(':',1)
                    nd_dict['type'] = k_value[0]
                    nd_dict['key'] = k_value[1]
                else:
                    nd_dict['type'] = default_tag_type
                    nd_dict['key'] = child.attrib['k']
                tags.append(nd_dict)
                
                pos = 0
                for node in element.iter('nd'):
                    way_node = {}
                    way_node['id'] = element.attrib['id']
                    way_node['node_id'] = node.attrib['ref']
                    way_node['position'] = pos
                    pos += 1
                    way_nodes.append(way_node)
                    
                return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: v for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w', "utf-8") as nodes_file, \
            codecs.open(NODE_TAGS_PATH, 'w', "utf-8") as nodes_tags_file, \
            codecs.open(WAYS_PATH, 'w', "utf-8") as ways_file, \
            codecs.open(WAY_NODES_PATH, 'w', "utf-8") as way_nodes_file, \
            codecs.open(WAY_TAGS_PATH, 'w', "utf-8") as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element, NODE_FIELDS, WAY_FIELDS, PROBLEMCHARS, 'regular', OSMFILE)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
if __name__ == '__main__':
    process_map(sample, validate=True)
