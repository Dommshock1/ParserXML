import xml.etree.ElementTree as ET
import os
import multiprocessing as mp
from multiprocessing import Manager 
import pandas as pd
import time
from tqdm.autonotebook import tqdm
from functools import partial
import glob

class ParserXML:
    
    def parseXML(self, filename, data_list):
        tree = ET.parse(filename)
        root = tree.getroot()
        json_data = {}
        self.parseElement(root, json_data)  
        data_list.append(json_data)

    def parseElement(self, element, parent_dict_new):   
        tag = element.tag
        text = element.text
        
        if len(element) == 0:
            if text:
                if tag in parent_dict_new: 
                    parent_dict_new[tag] = '{}, {}'.format(parent_dict_new[tag], text)
                else:
                    parent_dict_new[tag] =  text
        else:
            for child in element:
                self.parseElement(child, parent_dict_new)

def search_files(directory, maxLen=None):
    
    array_of_path = glob.glob(directory + '**/*.xml', recursive=True)

    if maxLen:
        array_of_path = array_of_path[:maxLen]
    
    return array_of_path

def saveFile(dataFrame, outputFilePath, outputFormat):
    
    print(f'Количество обработанных файлов {len(dataFrame)}. Сохранение данных...')
    start_time = time.time()
    
    if outputFormat == 'csv':
        dataFrame.to_csv(f'{outputFilePath}.{outputFormat}',index=False)
    elif outputFormat == 'xlsx': 
        dataFrame.to_excel(f'{outputFilePath}.{outputFormat}',index=False)

    end_time = time.time()
    print('Время выполнения процедуры:', end_time - start_time)
   
def parseXML(useMultiprocessing, inputFilePath, outputFilePath, outputFormat, maxLen):
    
    data_list = Manager().list()
    
    array_of_path = search_files(inputFilePath, maxLen)

    if useMultiprocessing:
        with mp.Pool(processes=mp.cpu_count()) as pool:
            for _ in tqdm(pool.imap(partial(ParserXML().parseXML, data_list = data_list), array_of_path), total = len(array_of_path)):
                pass
            pool.close()
            pool.join()
    else:
        for filename in  tqdm(array_of_path, total = len(array_of_path)):
           ParserXML().parseXML(filename, data_list) 
    
    df = pd.DataFrame.from_dict(list(data_list))        
    saveFile(df, outputFilePath, outputFormat)

if __name__ == '__main__':
    
    inputFilePath = 'C:\\Users\\User1\\Desktop\\NCT0000xxxx\\'
    outputFilePath = 'C:\\Users\\User1\\Desktop\\Output_file'
    outputFormat = 'csv'
    maxLen = 11
    useMultiprocessing = True
    
    parseXML(useMultiprocessing, inputFilePath, outputFilePath, outputFormat, maxLen)
    
                   
    

   
     
