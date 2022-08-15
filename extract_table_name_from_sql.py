from collections import Counter
import csv

def process_sql_file(file_name):
    file, string = open(file_name, 'r'), ''
    for line in file:
        # Remove single-line comments
        # line = line.split('//')[0]
        # line = line.split('--')[0]
        # line = line.split('#')[0]
        string += ' ' + line
    file.close()

    # remove multi-line comments:
    
    # while string.find('/*') > -1 and string.find('*/') > -1:
    #     l_multi_line = string.find('/*')
    #     r_multi_line = string.find('*/')
    #     string = string[:l_multi_line] + string[r_multi_line + 2:] 

    words = string.split()
    return words

def find_table_names(words):
    table_names = []
    previous_word = None
    for word in words:
        if previous_word == 'from' or previous_word == 'join':
            if word != '(':
                table_names.append(word)
        previous_word = word
    table_names = Counter(table_names)

    with open("output.csv", 'w') as fp:
        root = csv.writer(fp, delimiter='\t')
        root.writerow(["table", "count"])
        for i,j in table_names.items():
            root.writerow([i, j])
    print("CSV FILE CREATED")

def execute(file_name):
    words = process_sql_file(file_name)
    return find_table_names(words)

print(execute('sql_queries.sql'))


