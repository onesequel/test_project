import sys
sys.path.append(r"C:\Users\HP\AppData\Local\Programs\Python\Python39\Lib\site-packages\aws_lib_")
from aws_lib_.aws_ocr_main import main_call
import sys
import os
import re
import pandas as pd
import psycopg2
from datetime import datetime
import time
import shutil

conn = psycopg2.connect(user = "postgres", password = "1234",host = "localhost",port = "5432",database = "postgres")
cursor = conn.cursor()

def file_remover():
    for file in os.listdir(r'D:\One Drive\OneDrive\Desktop\suburban_excel'):
        os.remove(r'D:\One Drive\OneDrive\Desktop\suburban_excel\\'+file)


def create_excel():

    today_date = datetime.now().strftime('%d-%m-%Y')


    df1 = pd.read_sql(f'''SELECT * FROM failed_docs where failed_date = '{str(today_date)}' ''', conn)
    df1.to_excel(r'D:\One Drive\OneDrive\Desktop\suburban_excel\failed_and_new_docs.xlsx',index=False)
    # print(df1)

    df2 = pd.read_sql(f''' select*from suburban_table1 where extracted_data_date='{str(today_date)}'  ''',conn)
    df2.to_excel(r'D:\One Drive\OneDrive\Desktop\suburban_excel\suburban_table1.xlsx',index=False)

    df3 = pd.read_sql(f''' select*from suburban_table2 where extracted_data_date='{str(today_date)}'  ''',conn)
    df3.to_excel(r'D:\One Drive\OneDrive\Desktop\suburban_excel\suburban_table2.xlsx',index=False)

def Trigger(input_path):
    # print(input_path)
    output_path=r"D:\One Drive\OneDrive\Desktop\aws\New folder"
    text=''
    os.chdir(output_path)
    main_call(input_path)
    text = ''
    for file in os.listdir(output_path):
        if file.endswith(".txt") and 'inreadingorder' in file:
            file_path = f"{output_path}\\{file}"
            with open(file_path) as f:
                lines = f.read()
                # print(lines)
                text=text+"\n--------New Page-------\n"+lines  
                # text=text+lines        


    text2 =''
    for file in os.listdir(output_path):
        if file.endswith("text.txt"):
            file_path = f"{output_path}\\{file}"
            with open(file_path) as f:
                lines = f.read()
                # print(lines)
                text2 = text2+"\n--------New Page-------\n"+lines  
                # text=text+lines             
     

    test_description = ''
    for file in os.listdir(output_path):
        if file.endswith("table-0-tables-pretty.txt"):
            file_path = f"{output_path}\\{file}"
            with open(file_path) as f:
                lines = f.read()
                # print(lines)
                test_description=test_description+"\n--------New Page-------\n"+lines  
                # text=text+lines 


    sample_description = ''
    for file in os.listdir(output_path):
        if file.endswith("table-1-tables-pretty.txt"):
            file_path = f"{output_path}\\{file}"
            with open(file_path) as f:
                lines = f.read()
                # print(lines)
                sample_description=sample_description+"\n--------New Page-------\n"+lines  



    for file in os.listdir(r"D:\One Drive\OneDrive\Desktop\aws\New folder"):
        os.remove(r"D:\One Drive\OneDrive\Desktop\aws\New folder\\"+file)

    return text,text2,test_description,sample_description



def read_document(filepath):
    text1,text2,test_description,sample_description = Trigger(filepath)
    text = ' '.join(text1.split('\n'))
    # print(text)


    text2 = ' '.join(text2.split('\n'))
    # print(text2)

    if 'Referral Client Test Requisition Form' in text:

        try:
            client_code = re.search(r'Client\sCode.\s+.*?Referral',text2).group()
            client_code = re.sub(r'Client\s+Code.\s+|\s+Referral|Referral','',client_code)
        except:
            client_code = 'None'

        try:
            patient_name = re.search(r'Patient.s\s+Name\s+.*?\s+H\/O',text2).group()
            patient_name = re.sub(r'Patient.s\s+Name\s+.*?\)\s+|\s+H\/O|Patient\'s\s+Name\s+.*?\).\s+|Patient.s.*?\)\s+|Patient.s|Patient|\s\s|Name','',patient_name)
        except :
            try:
                patient_name = re.search(r'Patient.s\s+Name\s+.*?\).\s+Years',text).group()
                patient_name = 'None'
            except:
                patient_name = re.search(r'Patient.s\s+Name\s+.*?\s+Age',text).group()
                patient_name = re.sub(r'Patient.s\s+Name\s+.*?\)\:\s+|\s+Age|Patient.s.*?\)\s+|Patient.s|Patient|\s\s|Name','',patient_name).strip()

        # print(text2)
        try:
            collection_date_and_time = re.search(r'Collection\sDate\s+\&\s+Time\:\s+[0-9]+\/[0-9]+\/[0-9]+|Time.\s+[0-9]+\/[0-9]+\/[0-9]+',text2).group()
            collection_date_and_time = re.sub(r'Collection\sDate\s+\&\s+Time\:\s+|Time.','',collection_date_and_time).strip()

        except :
            collection_date_and_time = 'None'


        try:
            referring_doc_name = re.search(r'Referring\sDr.s\s+Name\:\s+Referring',text2).group()
            referring_doc_name = 'None'
        except:
            referring_doc_name = re.search(r'Referring\sDr.s\s+Name\:\s.*?Referring|Referring\sDr.s\s+Name\:\s.*?Clinical',text2).group()
            referring_doc_name = re.sub(r'\sSercimee\sFisation\sTime\s|Cilinical\sMistory.\s|Referring Dr\'s Name:\s+|Clinical|Referring|\sClÃ­nica|\sHistory.|History|\sHistery.|Histery.\s|Histery.','',referring_doc_name).strip()

        # print(text)
        try:
            age = re.search(r'\s+Age\:\s+[0-9]+',text).group()
            age = re.sub(r'Age\:\s+','',age).strip()
        except :
            age = 'None'

        try:
            lmp = re.search(r'LMP\:\s+[0-9]+\/[0-9]+\/[0-9]+',text).group()
            lmp = re.sub(r'LMP:\s+','',lmp)
        except:
            lmp = 'None'

        test_description = test_description.split('\n')[4:]
        test_names_from_test_description = []
        for line in test_description:
            if len(line.split('|')[-2].strip()) != 0:
                value = ' '.join(line.split('|')[2:-1]).strip()
                value = re.sub(r'\s+',' ',value)
                test_names_from_test_description.append(value)


        sample_description = sample_description.split('\n')[4:]
        test_names_from_sample_description = []

        for line2 in sample_description:
            value = line2.split('|')[1:-2][0]
            value = value.split(',')
            try:
                selected_or_not_selected = value[0].strip()
                sample_test_name = value[1].strip()

                if selected_or_not_selected.lower() == 'selected':
                    test_names_from_sample_description.append(sample_test_name)
            except :
                pass


        today_date = datetime.now().strftime('%d-%m-%Y')
        current_time = datetime.now().strftime("%I:%M %p")
        test_names_from_test_description = ','.join([s for s in test_names_from_test_description])
        test_names_from_sample_description = ','.join([s for s in test_names_from_sample_description])

        print("LMP:",lmp)
        print("Client Code:",client_code)
        print("Patient Name:",patient_name,'\n')
        print('Collection Date And Time:',collection_date_and_time)
        print("Referring Doc Name:",referring_doc_name)
        print("Age:",age)
        print('Test Names From Test Description:',test_names_from_test_description)
        print('Test Names From Sample Description:',test_names_from_sample_description)


        if lmp == 'None' and client_code == 'None' and patient_name == 'None' and collection_date_and_time == 'None' and referring_doc_name == 'None' and age == 'None' and len(test_names_from_test_description) == 0  and len(test_names_from_sample_description) == 0:
            query1 = f''' INSERT INTO suburban_table1 values('{str(lmp)}','{str(client_code)}','{str(patient_name)}','{str(collection_date_and_time)}','{str(referring_doc_name)}','{str(age)}','{str(test_names_from_test_description)}','{str(test_names_from_sample_description)}','{str(filepath)}','{str(today_date)}','{str(current_time)}','fail') '''
            shutil.move(filepath,r'C:\Users\HP\Music\suburban\failed')
        else:
            query1 = f''' INSERT INTO suburban_table1 values('{str(lmp)}','{str(client_code)}','{str(patient_name)}','{str(collection_date_and_time)}','{str(referring_doc_name)}','{str(age)}','{str(test_names_from_test_description)}','{str(test_names_from_sample_description)}','{str(filepath)}','{str(today_date)}','{str(current_time)}','success') '''
            shutil.move(filepath,r'C:\Users\HP\Music\suburban\processed')
        cursor.execute(query1)
        conn.commit()



    elif 'TEST REQUISITION FORM' in text:
        # print('yes')
        try:
            customer_name = re.search(r'CUSTOMER INFORMATION NAME: .*?DATE',text).group()
            customer_name = re.sub(r'CUSTOMER INFORMATION NAME: |\s+DATE','',customer_name)
        except:
            customer_name = 'None'

        try:
            date =re.search(r'DATE.\s[0-9]+\-[0-9]+\-[0-9]+',text).group()
            date = re.sub(r'DATE.','',date).strip()
        except:
            date = 'None'

        try:
            age = re.search(r'DOB\s\(AGE\).\s[0-9]+',text).group()
            age = re.sub(r'DOB|\(|\)|AGE|:','',age).strip()
        except:
            age = 'None'

        try:
            doctor_name = re.search(r'DOCTOR\sINFORMATION\sNAME.\s+.*MOBILE',text).group()
            doctor_name = re.sub(r'DOCTOR|INFORMATION|NAME|:|MOBILE','',doctor_name).strip()
        except :
            doctor_name = 'None'

        test_names_from_pathology = []
        try:
            pathology_text = re.search(r'(?sm)PATHOLOGY.*SONOGRAPHY',text1).group()
            pathology_text = re.sub(r'PATHOLOGY|DIGITAL X-RAY|SONOGRAPHY','',pathology_text)
            pathology_text = pathology_text.split('\n')

            for name in pathology_text:
                if len(name) != 0:
                    test_names_from_pathology.append(name.lower())
        except :
            pass


        test_names_from_pathology = ','.join([s for s in test_names_from_pathology])


        print('Customer Name:',customer_name)
        print('Date:',date)
        print('Age:',age)
        print('Doctor Name:',doctor_name)
        print('Test Names From Pathology:',test_names_from_pathology)
        print(type(test_names_from_pathology))

        today_date = datetime.now().strftime('%d-%m-%Y')
        current_time = datetime.now().strftime("%I:%M %p")

        if customer_name == 'None' and date == 'None' and age == 'None' and doctor_name == 'None' and len(test_names_from_pathology) == 0:
            query2 = f'''INSERT INTO suburban_table2 values('{str(customer_name)}','{str(date)}','{str(age)}','{str(doctor_name)}','{str(test_names_from_pathology)}','{str(filepath)}','{str(today_date)}','{str(current_time)}','fail') '''
            shutil.move(filepath,r'C:\Users\HP\Music\suburban\failed')
        else:
            query2 = f'''INSERT INTO suburban_table2 values('{str(customer_name)}','{str(date)}','{str(age)}','{str(doctor_name)}','{str(test_names_from_pathology)}','{str(filepath)}','{str(today_date)}','{str(current_time)}','success') '''
            shutil.move(filepath,r'C:\Users\HP\Music\suburban\processed')
        cursor.execute(query2)
        conn.commit()

    else:
        today_date = datetime.now().strftime('%d-%m-%Y')
        current_time = datetime.now().strftime("%I:%M %p")


        # file_name = re.sub(r'C..Users.HP.Music.suburban.excluded.','',filepath)
        query3 = f'''INSERT INTO failed_docs values('{str(filepath)}','{str(today_date)}','{str(current_time)}') '''
        shutil.move(filepath,r'C:\Users\HP\Music\suburban\failed')
        cursor.execute(query3)
        conn.commit()
        print('Please Check the Input File')





for file in os.listdir(r'C:\Users\HP\Music\suburban\main_files'):
    read_document(r'C:\Users\HP\Music\suburban\main_files\\'+file)


# read_document(r'C:\Users\HP\Music\suburban\main_files\DR._R_M_SARAOGI_-_MALAD_Done.pdf')
file_remover()
time.sleep(2)
create_excel()


