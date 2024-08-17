import os.path

from dependencies.spark import start_spark







def main():

    spark,log,config =start_spark(app_name='my_extract_job',files=['../configs/loan_config.json'])
    print(config)
    log.warn("etl is running")
    data=extracted_data(spark,config['input_dir'],config['file_name'])
    load_data(data,config['loan_details'])
    #data.show(5,False)
    # load_data(data)
    # print(config['input_dir']+config['file_name'])
    log.warn("the etl is finished")



def extracted_data(spark,input_dir,file_name):

    df=spark.read.option("delimiter",";").option("header","true").csv(input_dir+"/"+file_name)
    return df
def load_data(data,loan_details):
    data=data.write.parquet(loan_details,mode='overwrite')
    return data


if __name__ == '__main__':
    main()