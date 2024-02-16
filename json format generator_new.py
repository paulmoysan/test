import json as js
import pandas as pd

def generate_json (input_text, target_field_name):

    json_data = {}
    error_message ='success'
    mapping_lines = input_text.strip().split("\n")
    headers = ['text']
    df_mapping_lines = pd.DataFrame(mapping_lines, columns=headers)
    #print(mapping_lines)
    #print (df_mapping_lines)
    
    #extracting the depth of the field in the datamodel
    seperator ='/'
    df_mapping_lines['depth'] = df_mapping_lines['text'].str.count(seperator)
    #print (df_mapping_lines)
    
    #for each line giving the information if whether or not the line is a lookup condition
    
    #starting by adding a new field called "type"
    
    df_mapping_lines.loc[df_mapping_lines['text'].index[0], 'type'] = 'source field'
    df_mapping_lines.loc[df_mapping_lines['text'].str.contains('='), 'type'] = 'condition'
    df_mapping_lines.loc[df_mapping_lines['text'] =='WHERE', 'type'] = 'WHERE'
    df_mapping_lines.loc[df_mapping_lines['text'] =='AND', 'type'] = 'AND'
    
    #print (df_mapping_lines)
    
    conditions = df_mapping_lines['type'] == 'condition'
    df_conditions = df_mapping_lines[conditions]
    
    #print(df_conditions) 
      
    lookup_type_check = len(df_conditions['depth'].unique()) == 1
    #print(lookup_type_check)
    
    #determine the type of lookup to be written
    if len(df_conditions) < 2:
        look_up_type ='single_condition_lookup'
    
    if len(df_conditions) >= 2:
        look_up_type ='multiple_condition_lookup'
        
    if lookup_type_check == False:
        look_up_type ='complex array lookup'  
    
    condition_elements = []
    conditions_string = []
    rec_lookup_key =''
    rec_lookup_key_array = []
    
    
    for element in df_conditions['text']:
        condition_elements.append(element.split("/"))
    for element in condition_elements:
        if 'APartyKeyRef' in element[-1]:
            last_element = element[-1].split('=')
            element[-1]='['+ last_element[0].replace('"', '\'') + '=' + 'APartyKeyRef'+ ']'
        else:
            element[-1] = '['+ element[-1].replace('"', '\'') + ']'
        element_str = '.'.join(element)
        conditions_string.append(element_str)
  
    #producting lookup condition blocks
    if look_up_type == 'single_condition_lookup':
        rec_lookup_key = conditions_string[0]
    elif look_up_type  == 'multiple_condition_lookup':
        rec_lookup_key_array.append({
            "op": "and",
            "conditions": [str(cond)  for cond in conditions_string]
        })
    else:
        error_message ='cannot produce json, this is a '+ look_up_type
        
    #producing mapping block
    mapping_data =[]
    source_path = mapping_lines[0]
    source_path_list = source_path.split("/")
    source_field_name =''

    
    if look_up_type == 'single_condition_lookup' or look_up_type =='multiple_condition_lookup':
        if df_mapping_lines.loc[2,'depth'] - df_mapping_lines.loc[0,'depth'] == 0:
            source_field_name = source_path_list[-1]
        else:
            index = df_mapping_lines.loc[2,'depth'] - df_mapping_lines.loc[0,'depth'] - 1 
            source_path_list  = source_path_list[index:]  
            source_field_name = '.'.join(source_path_list)

    mapping_data.append({
        "targetFieldName": target_field_name,
        "sourceFieldName": source_field_name
    })
    
    
    if look_up_type  == 'single_condition_lookup':
        json_data["recLookupKey"] = rec_lookup_key
        json_data["mappingData"] = mapping_data
    elif look_up_type  == 'multiple_condition_lookup':
        json_data["recLookupKeyArray"] = rec_lookup_key_array
        json_data["mappingData"] = mapping_data

    return js.dumps(json_data, indent=4)

#check the output for few scenarii

#single_condition_lookup
input_string1 = '''agencyAccountBalance/openPosition/totalEquityValue
WHERE
agencyAccountBalance/openPosition/marginingPtfType="Equity"'''


#multiple_condition_lookup
input_string2 = '''agencyAccountBalance/openPosition/totalEquityValue
WHERE
agencyAccountBalance/openPosition/marginingPtfType="Equity"
AND
agencyAccountBalance/openPosition/source="Internal"'''

#complex array lookup
input_string3 = '''tradeOnOtc/product/productStream/observable/fixedValue
WHERE
tradeOnOtc/product/productStream/legType="InterestRate"
AND
tradeOnOtc/product/productStream/observable/observableType="FixedRate"'''

#csingle array lookup with reference to APartyKeyRef
input_string4 = '''tradeOnOtc/product/fixedValue
WHERE
tradeOnOtc/product/legType="InterestRate"
AND
tradeOnOtc/product/observableType=APartyKeyRef'''


# Generate JSON output
print("Output 1:")
print(generate_json(input_string1, 'case1'))
print("\nOutput 2:")
print(generate_json(input_string2, 'case2'))
print("Output 3:")
print(generate_json(input_string3, 'case3'))
print("Output 4:")
print(generate_json(input_string4, 'case4'))

df_input = pd.read_csv("data mapping.csv")
print(df_input)
#df['output_column'] = df['data mapping'].apply(generate_json)
#df['json'] = df['data mapping'].apply(generate_json)
df_input['json'] = df_input.apply(lambda row: generate_json(row['data mapping'], row['field name']), axis=1)
#print(df)
df_input.to_csv("output_file.csv", index=False)
df_output = pd.read_csv("output_file.csv")
print(df_output)
