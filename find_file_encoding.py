import chardet

file_path='/Users/macbookpro_2015/dev/python/data-proc-timer/test_data_1000.csv'


with open(file_path,"rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

