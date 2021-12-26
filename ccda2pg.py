import sys
import psycopg2
import xml.etree.ElementTree as ET

SQL_CMD = "insert into labresults(date, patient, name, value, unit, range) Values (%s, %s, %s, %s, %s, %s)"

conn = psycopg2.connect(
    host="your-host",
    database="health-vault",
    user="your-user",
    password="your-password")

tree = ET.parse(sys.argv[1])
report_date = sys.argv[2]

root = tree.getroot()
data = root.find("./{urn:hl7-org:v3}recordTarget/{urn:hl7-org:v3}patientRole/{urn:hl7-org:v3}patient/{urn:hl7-org:v3}name")
family = data.find("{urn:hl7-org:v3}family")
given = data.find("{urn:hl7-org:v3}given")
patient = f"{given.text} {family.text}"
print(patient)

for section in root.findall("./{urn:hl7-org:v3}component/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']/.."):
  for rec in section.findall("{urn:hl7-org:v3}text/{urn:hl7-org:v3}table/{urn:hl7-org:v3}thead/{urn:hl7-org:v3}tr"):
    data = rec.findall("{urn:hl7-org:v3}th")
    idx = 0
    name_idx = None
    value_idx = None
    range_idx = None
    unit_idx = None
    for d in data:
      if "name" in d.text.lower(): name_idx = idx
      if "component" in d.text.lower(): name_idx = idx
      if "range" in d.text.lower(): range_idx = idx
      if "value" in d.text.lower(): value_idx = idx
      if "result" in d.text.lower(): value_idx = idx
      idx =  idx + 1

  for rec in section.findall("{urn:hl7-org:v3}text/{urn:hl7-org:v3}table/{urn:hl7-org:v3}tbody/{urn:hl7-org:v3}tr"):
    data = rec.findall("{urn:hl7-org:v3}td")
    name = data[name_idx].text if name_idx != None and len(data) > name_idx else ""
    if name: name = name.upper()
    value = data[value_idx].text if value_idx != None and len(data) > value_idx  else ""
    try:
      value = float(value)
    except (TypeError, ValueError):
      value = None
    range = data[range_idx].text if range_idx != None and len(data) > range_idx  else ""
    unit = data[unit_idx].text if unit_idx != None and len(data) > unit_idx  else ""

    if name and value:
      print(f"{name}, {value}, {unit}, {range}")
      try:
        cur = conn.cursor()
        cur.execute(SQL_CMD, (report_date, patient, name, float(value), unit, range))
        conn.commit()
      except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
      finally:
        cur.close()

  conn.close()
