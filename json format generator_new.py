import json
import pandas as pd
#import numpy as np



def generate_json(field_name, input_string):
   #creating a list for each line
   conditions = input_string.strip().split("\n")
   #print (conditions)
   json_data = {}

   # Initialize lists to store conditions and mapping data
   rec_lookup_key_array = [] #in case of a lookup on multiple conditions
   rec_lookup_key ='' ##in case of a lookup on a single condition
   mapping_data = []

   # get the index for the "WHERE" string
   where_index = conditions.index("WHERE")
   #print(where_index)

   #create a list with the element within the WHERE clause
   where_conditions = conditions[where_index + 1:]
   #print(where_conditions)
   and_conditions = []

   # Extracting conditions separated by "AND"
   for condition in where_conditions:
       if condition.startswith("AND"):
           pass
       else:
           and_conditions.append(condition)

   #print (and_conditions)

   and_condition_elements =[]

   for and_cond in and_conditions:
       and_condition_elements.append(and_cond.split("/"))

   #print (and_condition_elements)

   and_conditions_string = []

   for element in and_condition_elements:
           element[-1] = '['+ element[-1].replace('"', '\'') + ']'
           element_str = '.'.join(element)
           and_conditions_string.append(element_str)

   #print (and_conditions_string)

   if len(and_conditions_string) == 1:
       rec_lookup_key = and_conditions_string[0]
   else:
       rec_lookup_key_array.append({
           "op": "and",
           "conditions": [str(cond)  for cond in and_conditions_string]
       })
   #print(rec_lookup_key_array)
   #print(rec_lookup_key)

   source_path = conditions[0]
   source_path_list = source_path.split("/")
   source_field_name = source_path_list[-1]

   mapping_data.append({
       "targetFieldName": field_name,
       "sourceFieldName": source_field_name
   })
   #print(mapping_data)

   if len(and_conditions_string) == 1:
       json_data["recLookupKey"] = rec_lookup_key
       json_data["mappingData"] = mapping_data
   else:
       json_data["recLookupKeyArray"] = rec_lookup_key_array
       json_data["mappingData"] = mapping_data

   return json.dumps(json_data, indent=4)

# Example input string
input_string1 = '''agencyAccountBalance/valuation/netLiquidatingValue
WHERE
agencyAccountBalance/valuation/type="Client"'''

input_string2 = '''agencyAccountBalance/openPosition/openTradeEquityValue
WHERE
agencyAccountBalance/openPosition/marginingPtfType="Equity"
AND
agencyAccountBalance/openPosition/source="Internal"'''

# Generate JSON output
output_json1 = generate_json('toto',input_string1)
output_json2 = generate_json('tata',input_string2)

print("Output 1:")
print(output_json1)

print("\nOutput 2:")
print(output_json2)


df = pd.read_csv("data mapping.csv")
print(df)

#df['output_column'] = df['data mapping'].apply(generate_json)
#df['json'] = df['data mapping'].apply(generate_json)
df['json'] = df.apply(lambda row: generate_json(row['field name'], row['data mapping']), axis=1)
print(df)
df.to_csv("output_file.csv", index=False)
